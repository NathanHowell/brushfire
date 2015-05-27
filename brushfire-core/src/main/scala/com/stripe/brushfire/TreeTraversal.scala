package com.stripe.brushfire

import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.math.Ordering
import scala.util.Random

import com.twitter.algebird._

/**
 * A `TreeTraversal` provides a way to find an ordered list of nodes in a tree
 * that match  a row. The actual order they are returned in depends on the
 * implementation.
 */
trait TreeTraversal[K, V, T, A] {

  /**
   * Limit the maximum number of leaves returned from `find` to `n`.
   */
  def limitTo(n: Int): TreeTraversal[K, V, T, A] =
    LimitedTreeTraversal(this, n)

  /**
   * Find the [[LeafNode]] that best fits `row` in the tree.  Generally, the
   * path from `tree.root` to the resulting leaf node will be along *only*
   * `true` predicates. However, when multiple predicates are `true` in a
   * [[SplitNode]], the actual choice of which one gets chosen is left to the
   * particular implementation of `TreeTraversal`.
   *
   * @param tree the decision tree to search in
   * @param row  the row/instance we're trying to match with a leaf node
   * @return the leaf nodes that best match the row
   */
  def find(tree: AnnotatedTree[K, V, T, A], row: Map[K, V]): Stream[LeafNode[K, V, T, A]] =
    find(tree.root, row)

  /**
   * Find the [[LeafNode]] that best fits `row` and is a descendant of `init`.
   * Generally, the path from `init` to the resulting leaf node will be along
   * *only* `true` predicates. However, when multiple predicates are `true` in
   * a [[SplitNode]], the actual choice of which one gets chosen is left to the
   * particular implementation of `TreeTraversal`.
   *
   * @param init the initial node to start from
   * @param row  the row/instance we're trying to match with a leaf node
   * @return the leafs node that match the row
   */
  def find(node: Node[K, V, T, A], row: Map[K, V]): Stream[LeafNode[K, V, T, A]]
}

object TreeTraversal {

  def find[K, V, T, A](tree: AnnotatedTree[K, V, T, A], row: Map[K, V])(implicit traversal: TreeTraversal[K, V, T, A]): Stream[LeafNode[K, V, T, A]] =
    traversal.find(tree, row)

  /**
   * Performs a depth-first traversal of the tree, returning all matching leaf
   * nodes.
   */
  implicit def depthFirst[K, V, T, A]: TreeTraversal[K, V, T, A] =
    DepthFirstTreeTraversal(identity)

  /**
   * A depth first search for matching leaves, randomly choosing the order of
   * child candidate nodes to traverse at each step. Since it is depth-first,
   * after a node is chosen to be traversed, all of the matching leafs that
   * descend from that node are traversed before moving onto the node's
   * sibling.
   */
  def randomDepthFirst[K, V, T, A](rng: Random = Random): TreeTraversal[K, V, T, A] =
    DepthFirstTreeTraversal(rng.shuffle(_))

  /**
   * A depth-first search for matching leaves, where the candidate child nodes
   * for a given parent node are traversed in reverse order of their
   * annotations. This means that if we have multiple valid candidate children,
   * we will traverse the child with the largest annotation first.
   */
  def weightedDepthFirst[K, V, T, A: Ordering]: TreeTraversal[K, V, T, A] =
    DepthFirstTreeTraversal(_.sortBy(_.annotation)(Ordering[A].reverse))

  /**
   * A depth-first search for matching leaves, where the candidate child leaves
   * of a parent node are randomly shuffled, but with nodes with higher weight
   * being given a higher probability of being ordered earlier. This is
   * basically a mix between [[randomDepthFirst]] and [[weightedDepthFirst]].
   */
  def probabilisticWeightedDepthFirst[K, V, T, A <% Double](rng: Random = Random): TreeTraversal[K, V, T, A] =
    DepthFirstTreeTraversal(probabilisticShuffle(_, rng)(_.annotation))

  /**
   * Given a weighted list `xs`, this shuffles the list, but elements with
   * higher weights have a higher probabilitiy of being ordered first.
   */
  private[brushfire] def probabilisticShuffle[A](xs: List[A], rng: Random = scala.util.Random)(getWeight: A => Double): List[A] = {

    @tailrec
    def loop(sum: Double, acc: SortedMap[Double, Int], order: Vector[A], as: List[A]): Vector[A] =
      as match {
        case a :: tail =>
          val sum0 = sum + getWeight(a)
          val acc0 = acc + (sum0 -> order.size)
          val newHead = acc.from(rng.nextDouble * sum0).headOption
          newHead match {
            case Some((k, i)) =>
              val acc0 = acc + (sum0 -> i) + (k -> order.size)
              loop(sum0, acc0, order.updated(i, a) :+ order(i), tail)
            case None =>
              val acc0 = acc + (sum0 -> order.length)
              loop(sum0, acc0, order :+ a, tail)
          }

        case Nil =>
          order.reverse
      }

    loop(0D, SortedMap.empty, Vector.empty, xs).toList
  }
}

case class DepthFirstTreeTraversal[K, V, T, A](order: List[Node[K, V, T, A]] => List[Node[K, V, T, A]])
    extends TreeTraversal[K, V, T, A] {
  def find(start: Node[K, V, T, A], row: Map[K, V]): Stream[LeafNode[K, V, T, A]] = {
    def loop(stack: List[Node[K, V, T, A]]): Stream[LeafNode[K, V, T, A]] =
      stack match {
        case Nil =>
          Stream.empty
        case (leaf @ LeafNode(_, _, _)) :: rest =>
          leaf #:: loop(rest)
        case (split @ SplitNode(_)) :: rest =>
          val newStack = split.findChildren(row) match {
            case Nil => rest
            case node :: Nil => node :: rest
            case candidates => order(candidates) ::: rest
          }
          loop(newStack)
      }

    loop(start :: Nil)
  }
}

case class LimitedTreeTraversal[K, V, T, A](traversal: TreeTraversal[K, V, T, A], limit: Int)
    extends TreeTraversal[K, V, T, A] {
  require(limit > 0, "limit must be greater than 0")

  def find(node: Node[K, V, T, A], row: Map[K, V]): Stream[LeafNode[K, V, T, A]] =
    traversal.find(node, row).take(limit)
}