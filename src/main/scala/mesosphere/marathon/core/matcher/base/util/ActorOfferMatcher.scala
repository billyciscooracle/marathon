package mesosphere.marathon.core.matcher.base.util

import akka.actor.ActorRef
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.matcher.base.OfferMatcher.MatchedInstanceOps
import mesosphere.marathon.state.{ PathId, Timestamp }
import org.apache.mesos.Protos.Offer

import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._

/**
  * Provides a thin wrapper around an OfferMatcher implemented as an actors.
  */
class ActorOfferMatcher(
    now: () => Timestamp,
    actorRef: ActorRef,
    override val precedenceFor: Option[PathId],
    scheduler: akka.actor.Scheduler) extends OfferMatcher {
  def matchOffer(deadline: Timestamp, offer: Offer): Future[MatchedInstanceOps] = {
    val timeout: FiniteDuration = now().until(deadline)

    if (timeout.duration <= ActorOfferMatcher.MinimalOfferComputationTime) {
      // if deadline is exceeded return no match
      Future.successful(MatchedInstanceOps.noMatch(offer.getId))
    } else {
      val p = Promise[MatchedInstanceOps]()
      scheduler.scheduleOnce(timeout) {
        if (p.trySuccess(MatchedInstanceOps.noMatch(offer.getId))) {
          logger.warn(s"Could not process offer '${offer.getId.getValue}' in time. (See --offer_matching_timeout)")
        }
      }
      actorRef ! ActorOfferMatcher.MatchOffer(deadline, offer, p)
      p.future
    }
  }

  override def toString: String = s"ActorOfferMatcher($actorRef)"
}

object ActorOfferMatcher {

  // Do not start a offer matching if there is less time than this minimal time
  // Optimization to prevent timeouts
  val MinimalOfferComputationTime: FiniteDuration = 50.millis

  /**
    * Send to an offer matcher to request a match.
    *
    * This should always be replied to with a LaunchTasks message.
    * TODO(jdef) pods will probably require a non-LaunchTasks message
    *
    * @param matchingDeadline Don't match after deadline.
    * @param remainingOffer ???
    * @param promise The promise to fullfil with match.
    */
  case class MatchOffer(matchingDeadline: Timestamp, remainingOffer: Offer, promise: Promise[MatchedInstanceOps])
}
