package sparkRunner

import org.bson.types.ObjectId

case class TrendObj(_id: ObjectId,
                    name: String,
                    location: String,
                    rank: Int,
                    as_of: String) {}

object TrendObj {
  def apply(name: String,
            location: String,
            rank: Int,
            as_of: String) : TrendObj = TrendObj(new ObjectId(),
                                                name,
                                                location,
                                                rank,
                                                as_of)
}