package com.stripe.brushfire

import com.twitter.algebird.Monoid
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks

class TreeAnnotationSpec extends WordSpec with PropertyChecks {
  import TreeGenerators._

  private def leafs[K, V, T, A]: Node[K, V, T, A] => Stream[LeafNode[K, V, T, A]] = {
    case leaf @ LeafNode(_, _, _) => Stream(leaf)
    case split @ SplitNode(_, xs) => xs.toStream.flatMap { case (_, _, node) => leafs(node) }
  }

  private def arbitraryFeatures: Boolean => Gen[Seq[Int]] = {
    case true  => Gen.nonEmptyListOf(Gen.frequency((7, Gen.choose(0, 9)), (3, Gen.choose(8, 9))))
    case false => Gen.nonEmptyListOf(Gen.frequency((7, Gen.choose(0, 9)), (3, Gen.choose(0, 1))))
  }

  private def validateExpansion(times: Int)(instances: Seq[Instance[Int, Map[Int, Long], Map[Boolean, Long]]]): Unit = {
    val (targets, metadata) = Monoid.sum(instances.view.map(i => (i.target, i.metadata)))
    val tree = Tree.singleton[Int, Long, Map[Boolean, Long], Int](targets, metadata)
    leafs(tree.root).foreach { leaf =>
      val splitter = BinarySplitter[Long, Map[Boolean, Long]](v => IsPresent(Some(LessThan(v))))
      val evaluator = ChiSquaredEvaluator[Long, Boolean, Long, Int]
      val stopper = FrequencyStopper[Boolean](1000, 2)
      val annotator = Annotator.identity[Int]
      val exp = Tree.expand[Int, Int, Long, Map[Boolean, Long], Int](
        times, leaf, splitter, evaluator, stopper, annotator, instances)

      assert(Monoid.sum(leafs(tree.root).map(_.target)) === Monoid.sum(leafs(exp).map(_.target)))
      assert(tree.root.annotation === exp.annotation)
    }
  }

  "Tree Annotations" should {
    "root annotation is equal to sum of leaf annotations" in {
      forAll(arbitrary[Tree[Unit, Unit, Unit, Int]]) { tree =>
        val ann0 = Monoid.sum(leafs(tree.root).map(_.annotation))
        assert(tree.root.annotation === ann0)
      }
    }

    "expand recalculates annotations when inducing splits" in {
      val arbitraryInstance = for {
        target <- arbitrary[Boolean]
        features <- arbitraryFeatures(target)
        annotation <- arbitrary[Int]
      } yield {
        Instance(metadata = annotation, features = Monoid.sum(features.map(f => Map(f -> 1L))), Map(target -> 1L))
      }

      val arbitraryInstances = for {
        count <- Gen.choose(5, 100)
        instances <- Gen.listOfN(count, arbitraryInstance)
      } yield {
        instances
      }

      forAll(arbitraryInstances)(validateExpansion(2))
    }
  }
}
