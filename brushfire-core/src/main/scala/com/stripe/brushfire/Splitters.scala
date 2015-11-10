package com.stripe.brushfire

import com.twitter.algebird._

case class BinarySplitter[V, T: Monoid](partition: V => Predicate[V])
    extends Splitter[V, T] {

  type S = Map[V, T]
  def create(value: V, target: T) = Map(value -> target)

  val semigroup = implicitly[Semigroup[Map[V, T]]]

  def split[A](parent: T, stats: Map[V, T], annotation: A) = {
    stats.keys.map { v =>
      val predicate = partition(v)
      val (trues, falses) = stats.partition { case (v, d) => predicate(Some(v)) }
      BinarySplit(predicate, Monoid.sum(trues.values), Monoid.sum(falses.values), annotation)
    }
  }
}

case class RandomSplitter[V, T](original: Splitter[V, T])
    extends Splitter[V, T] {
  type S = original.S
  val semigroup = original.semigroup
  def create(value: V, target: T): S = original.create(value, target)
  def split[A](parent: T, stats: S, annotation: A) = {
    scala.util.Random.shuffle(original.split(parent, stats, annotation)).headOption
  }
}

case class BinnedSplitter[V, T](original: Splitter[V, T])(fn: V => V)
    extends Splitter[V, T] {
  type S = original.S
  def create(value: V, target: T) = original.create(fn(value), target)
  val semigroup = original.semigroup
  def split[A](parent: T, stats: S, annotation: A) = {
    original.split(parent, stats, annotation)
  }
}

case class QTreeSplitter[T: Monoid](k: Int)
    extends Splitter[Double, T] {

  type S = QTree[T]

  val semigroup = new QTreeSemigroup[T](k)
  def create(value: Double, target: T) = QTree(value -> target)

  def split[A](parent: T, stats: QTree[T], annotation: A) = {
    findAllThresholds(stats).map { threshold =>
      val predicate = LessThan(threshold)
      val leftDist = stats.rangeSumBounds(stats.lowerBound, threshold)._1
      val rightDist = stats.rangeSumBounds(threshold, stats.upperBound)._1
      BinarySplit(predicate, leftDist, rightDist, annotation)
    }
  }

  def findAllThresholds(node: QTree[T]): Iterable[Double] = {
    node.lowerChild.toList.flatMap { l => findAllThresholds(l) } ++
      node.upperChild.toList.flatMap { u => findAllThresholds(u) } ++
      findThreshold(node).toList
  }

  def findThreshold(node: QTree[T]): Option[Double] = {
    for (l <- node.lowerChild; u <- node.upperChild)
      yield l.upperBound
  }
}

case class SparseSplitter[V, T: Group]() extends Splitter[V, T] {
  type S = T
  def create(value: V, target: T) = target
  val semigroup = implicitly[Semigroup[T]]
  def split[A](parent: T, stats: T, annotation: A) =
    BinarySplit(IsPresent[V](None), stats, Group.minus(parent, stats), annotation) :: Nil
}

case class SpaceSaverSplitter[V, L](capacity: Int = 1000)
    extends Splitter[V, Map[L, Long]] {

  type S = Map[L, SpaceSaver[V]]
  val semigroup = implicitly[Semigroup[S]]

  def create(value: V, target: Map[L, Long]) = target.mapValues { c =>
    Semigroup.intTimes(c, SpaceSaver(capacity, value))
  }

  def split[A](parent: Map[L, Long], stats: S, annotation: A) = {
    stats
      .values
      .flatMap { _.counters.keys }.toSet
      .map { v: V =>
        val mins = stats.mapValues { ss => ss.frequency(v).min }
        BinarySplit(EqualTo(v), mins, Group.minus(parent, mins), annotation)
      }
  }
}

case class BinarySplit[V, T, A](
  predicate: Predicate[V],
  leftDistribution: T,
  rightDistribution: T,
  annotation: A)
    extends Split[V, T, A] {
  def predicates: List[(Predicate[V], T, A)] =
    List((predicate, leftDistribution, annotation), (Not(predicate), rightDistribution, annotation))
}
