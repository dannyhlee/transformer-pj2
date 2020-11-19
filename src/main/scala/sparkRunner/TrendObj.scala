package sparkRunner

import org.bson.types.ObjectId

import scala.annotation.tailrec

/**
 * TrendObj is a case class to encapsulate singular trend topics from each API requests 50 topics with
 * their individual location information and rank at that time (`as_of` field)
 *
 * @param _id MongoDB id
 * @param name Trend Topic
 * @param location City it trended in
 * @param rank The numerical rank
 * @param as_of timestamp of event
 */
case class TrendObj(_id: ObjectId, name: String, location: String, rank: Int, as_of: String) {}

object TrendObj {
  @tailrec
  def apply(name: String, location: String, rank: Int, as_of: String) : TrendObj =
    TrendObj(name, location, rank, as_of)
}