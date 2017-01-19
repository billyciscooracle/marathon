package mesosphere.marathon
package api.validation

import mesosphere.UnitTest
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.state.{ AppDefinition, Container, DiskType, PathId, PersistentVolumeInfo, Residency, UnreachableStrategy, UpgradeStrategy, Volume }
import com.wix.accord.scalatest.ResultMatchers
import org.apache.mesos.Protos.Volume.Mode

import scala.concurrent.duration._

class AppDefinitionValidationTest extends UnitTest with ResultMatchers {

  "AppDefinition" when {
    "created with the default unreachable strategy" should {
      "be valid" in {
        val f = new Fixture()
        val app = AppDefinition(id = PathId("/test"), cmd = Some("sleep 1000"))

        val validation = f.appDefinitionValidator(app)
        validation shouldBe aSuccess

      }
    }

    "created with unreachableStrategy disabled" should {
      "be invalid" in {
        val f = new Fixture()
        val app = AppDefinition(
          id = PathId("/test"),
          cmd = Some("sleep 1000"),
          unreachableStrategy = UnreachableStrategy(0.second))

        f.appDefinitionValidator(app) shouldBe aSuccess
      }
    }

    "created with unreachableStrategy inactiveAfter enabled for resident tasks" should {
      "be invalid" in {

        val f = new Fixture()
        val app = AppDefinition(
          id = PathId("/test"),
          cmd = Some("sleep 1000"),
          container = Some(
            Container.Docker(
              image = "very-image",
              volumes = Seq(
                Volume(
                  containerPath = "path-to-container",
                  hostPath = None,
                  mode = Mode.RW,
                  persistent = Some(
                    PersistentVolumeInfo(
                      100L,
                      None,
                      DiskType.Root)),
                  external = None)))),
          residency = Some(Residency.defaultResidency),
          upgradeStrategy = UpgradeStrategy.forResidentTasks,
          unreachableStrategy = UnreachableStrategy(5.second))
        val expectedViolation = RuleViolationMatcher(constraint = "unreachableStrategy inactiveAfter must be disabled for resident tasks")

        f.appDefinitionValidator(app) should failWith(expectedViolation)
      }

    }

  }

  class Fixture {
    val appDefinitionValidator = AppDefinition.validAppDefinition(Set.empty)(PluginManager.None)
  }
}
