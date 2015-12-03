package com.stripe.brushfire
package spark

import com.twitter.algebird._
import com.twitter.algebird.spark._
import com.twitter.bijection.Injection

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import java.util.Random

import scala.reflect.ClassTag

case class Trainer[M, K: Ordering, V, T: Monoid: ClassTag, A: Monoid: ClassTag](
    trainingData: RDD[Instance[M, Map[K, V], T]],
    sampler: Sampler[M, K],
    annotator: Annotator[M, A],
    trees: RDD[(Int, Tree[K, V, T, A])]) {
  private def context: SparkContext = trainingData.context

  // A scored split for a particular feature.
  type ScoredSplit = (K, Split[V, T, A], Double)

  private val emptySplits: RDD[(Int, Map[Int, ScoredSplit])] = context
    .parallelize(Seq.tabulate(sampler.numTrees)(_ -> Map.empty[Int, ScoredSplit]))
    .cache()

  private val emptyExpansions: RDD[(Int, List[(Int, Node[K, V, T, A])])] = context
    .parallelize(Seq.tabulate(sampler.numTrees)(_ -> List.empty[(Int, Node[K, V, T, A])]))
    .cache()

  def saveAsTextFile(path: String)(implicit inj: Injection[Tree[K, V, T, A], String]): Trainer[M, K, V, T, A] = {
    trees
      .map {
        case (i, tree) =>
          s"$i\t${inj(tree)}"
      }
      .saveAsTextFile(path)
    this
  }

  /**
   * Update the leaves of the current trees from the training set.
   *
   * The leaves target distributions will be set to the summed distributions of the instances
   * in the training set that would get classified to them. Often used to initialize an empty tree.
   */
  def updateTargets: Trainer[M, K, V, T, A] = {
    type LeafId = (Int, Int)

    val treeMap: scala.collection.Map[Int, Tree[K, V, T, A]] = trees.collectAsMap()

    val collectLeaves: Instance[M, Map[K, V], T] => Iterable[(LeafId, T)] = { instance =>
      for {
        (treeIndex, tree) <- treeMap.toList
        repetition = sampler.timesInTrainingSet(instance.metadata, treeIndex)
        i <- 1 to repetition
        leafIndex <- tree.leafIndexFor(instance.features).toList
      } yield {
        (treeIndex, leafIndex) -> instance.target
      }
    }

    val newTrees = trainingData
      .flatMap(collectLeaves)
      .algebird
      .sumByKey[LeafId, T]
      .map {
        case ((treeIndex, leafIndex), target) =>
          treeIndex -> Map(leafIndex -> target)
      }
      .algebird
      .sumByKey[Int, Map[Int, T]]
      .map {
        case (treeIndex, leafTargets) =>
          val newTree =
            treeMap(treeIndex).updateByLeafIndex { index =>
              leafTargets.get(index).map(LeafNode(index, _))
            }

          treeIndex -> newTree
      }
      .cache()

    copy(trees = newTrees)
  }

  def expandInMemory(times: Int)(implicit evaluator: Evaluator[V, T, A], splitter: Splitter[V, T], stopper: Stopper[T]): Trainer[M, K, V, T, A] = {
    type LeafId = (Int, Int)

    val treeMap: scala.collection.Map[Int, Tree[K, V, T, A]] = trees.collectAsMap()

    def sampleInstances(rng: Random): Instance[M, Map[K, V], T] => Iterable[(LeafId, Instance[M, Map[K, V], T])] = { instance =>
      for {
        (treeIndex, tree) <- treeMap.toList
        repetition = sampler.timesInTrainingSet(instance.metadata, treeIndex)
        i <- 1 to repetition
        leaf <- tree.leafFor(instance.features).toList
        if stopper.shouldSplit(leaf.target) && rng.nextDouble < stopper.samplingRateToSplitLocally(leaf.target)
      } yield {
        (treeIndex, leaf.index) -> instance
      }
    }

    val newTrees = trainingData
      .mapPartitionsWithIndex {
        case (idx, instances) =>
          val rng: Random = new Random(idx)
          instances.flatMap(sampleInstances(rng))
      }
      .groupByKey()
      .map {
        case ((treeIndex, leafIndex), instances) =>
          val (target, annotation) = Monoid.sum(instances.map { i => (i.target, annotator.create(i.metadata)) })
          val leaf = LeafNode[K, V, T, A](0, target, annotation)
          val expanded = Tree.expand(times, leaf, splitter, evaluator, stopper, annotator, instances)
          treeIndex -> List(leafIndex -> expanded)
      }
      .union(emptyExpansions)
      .algebird
      .sumByKey[Int, List[(Int, Node[K, V, T, A])]]
      .map {
        case (treeIndex, leafExpansions) =>
          val expansions = leafExpansions.toMap
          val newTree = treeMap(treeIndex)
            .updateByLeafIndex(expansions.get)
          treeIndex -> newTree
      }
      .cache()

    copy(trees = newTrees)
  }

  def expandTimes(times: Int)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T, A], stopper: Stopper[T]): Trainer[M, K, V, T, A] = {
    def loop(trainer: Trainer[M, K, V, T, A], i: Int): Trainer[M, K, V, T, A] =
      if (i > 0) loop(trainer.expand, i - 1)
      else trainer

    loop(updateTargets, times)
  }

  /**
   * Grow the trees by splitting all the leaf nodes as the stopper allows.
   */
  private def expand(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T, A], stopper: Stopper[T]): Trainer[M, K, V, T, A] = {
    // Our bucket has a tree index, leaf index, and feature.
    type Bucket = (Int, Int, K)

    implicit object ScoredSplitSemigroup extends Semigroup[ScoredSplit] {
      def plus(a: ScoredSplit, b: ScoredSplit) =
        if (b._3 > a._3) b else a
    }

    val treeMap: scala.collection.Map[Int, Tree[K, V, T, A]] = trees.collectAsMap()

    val collectFeatures: Instance[M, Map[K, V], T] => Iterable[(Bucket, splitter.S)] = { instance =>
      val features = instance.features.mapValues(splitter.create(_, instance.target))
      for {
        (treeIndex, tree) <- treeMap.toList
        repetition = sampler.timesInTrainingSet(instance.metadata, treeIndex)
        i <- 1 to repetition
        leaf <- tree.leafFor(instance.features).toList
        if stopper.shouldSplit(leaf.target) && stopper.shouldSplitDistributed(leaf.target)
        (feature, stats) <- features
        if sampler.includeFeature(instance.metadata, feature, treeIndex, leaf.index)
      } yield {
        (treeIndex, leaf.index, feature) -> stats
      }
    }

    val split: (Bucket, splitter.S) => Iterable[(Int, Map[Int, ScoredSplit])] = { (bucket, stats) =>
      val (treeIndex, leafIndex, feature) = bucket
      for {
        leaf <- treeMap(treeIndex).leafAt(leafIndex).toList
        rawSplit <- splitter.split(leaf.target, stats, leaf.annotation)
      } yield {
        val (split, goodness) = evaluator.evaluate(rawSplit)
        treeIndex -> Map(leafIndex -> (feature, split, goodness))
      }
    }

    val growTree: (Int, Map[Int, ScoredSplit]) => (Int, Tree[K, V, T, A]) = { (treeIndex, leafSplits) =>
      val newTree =
        treeMap(treeIndex)
          .growByLeafIndex { index =>
            for {
              (feature, split, _) <- leafSplits.get(index).toList
              (predicate, target, annotation) <- split.predicates
            } yield {
              (feature, predicate, target, annotation)
            }
          }
      treeIndex -> newTree
    }

    // Ugh. We could also wrap splitter.S in some box...
    implicit val existentialClassTag: ClassTag[splitter.S] =
      scala.reflect.classTag[AnyRef].asInstanceOf[ClassTag[splitter.S]]

    val newTrees = trainingData
      .flatMap(collectFeatures)
      .reduceByKey(splitter.semigroup.plus(_, _))
      .flatMap(split.tupled)
      .union(emptySplits)
      .algebird
      .sumByKey[Int, Map[Int, ScoredSplit]]
      .map(growTree.tupled)
      .cache()

    copy(trees = newTrees)
  }
}

object Trainer {
  val MaxParallelism = 20

  def apply[M, K: Ordering, V, T: Monoid: ClassTag, A: Monoid: ClassTag](
      trainingData: RDD[Instance[M, Map[K, V], T]],
      sampler: Sampler[M, K],
      annotator: Annotator[M, A]): Trainer[M, K, V, T, A] = {
    val sc = trainingData.context
    val initialTrees = Vector.tabulate(sampler.numTrees) { _ -> Tree.singleton[K, V, T, A](Monoid.zero[T], Monoid.zero[A]) }
    val parallelism = scala.math.min(MaxParallelism, sampler.numTrees)
    Trainer(
      trainingData,
      sampler,
      annotator,
      sc.parallelize(initialTrees, parallelism))
  }
}
