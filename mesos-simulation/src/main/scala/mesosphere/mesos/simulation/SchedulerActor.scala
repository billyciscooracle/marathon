package mesosphere.mesos.simulation

import akka.actor.{ Actor, Stash }
import akka.event.LoggingReceive
import mesosphere.marathon.stream._
import org.apache.mesos.Protos.{ FrameworkID, MasterInfo, Offer, TaskStatus }
import org.apache.mesos.{ Scheduler, SchedulerDriver }
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq

object SchedulerActor {
  private val log = LoggerFactory.getLogger(getClass)

  case class Registered(
    frameworkId: FrameworkID,
    master: MasterInfo)

  case class ResourceOffers(offers: Seq[Offer])
}

class SchedulerActor(scheduler: Scheduler) extends Actor with Stash {
  import SchedulerActor._

  var driverOpt: Option[SchedulerDriver] = None

  def receive: Receive = waitForDriver

  def waitForDriver: Receive = LoggingReceive.withLabel("waitForDriver") {
    case driver: SchedulerDriver =>
      log.info("received driver")
      driverOpt = Some(driver)
      context.become(handleCmds(driver))
      unstashAll()

    case _ => stash()
  }

  def handleCmds(driver: SchedulerDriver): Receive = LoggingReceive.withLabel("handleCmds") {
    case Registered(frameworkId, masterInfo) =>
      scheduler.registered(driver, frameworkId, masterInfo)

    case ResourceOffers(offers) =>
      scheduler.resourceOffers(driver, offers)

    case status: TaskStatus =>
      scheduler.statusUpdate(driver, status)
  }

  override def postStop(): Unit = {
    driverOpt.foreach { driver => scheduler.disconnected(driver) }
  }
}
