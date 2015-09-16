package org.elasticsearch.gradle

import com.carrotsearch.gradle.randomizedtesting.RandomizedTestingTask
import org.apache.maven.BuildFailureException
import org.gradle.api.Task
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.Input
import org.gradle.util.ConfigureUtil

/**
 * Runs integration tests, but first starts an ES cluster,
 * and passes the ES cluster info as parameters to the tests.
 */
class IntegTestTask extends RandomizedTestingTask {

    ClusterConfiguration clusterConfig = new ClusterConfiguration()

    IntegTestTask() {
        project.afterEvaluate {
            ClusterFormationTasks.addTasks(this, clusterConfig)
            configure {
                parallelism '1'
                sysProp 'tests.cluster', "localhost:${clusterConfig.transportPort}"
            }
        }
    }

    @Input
    void cluster(Closure closure) {
        ConfigureUtil.configure(closure, clusterConfig)
    }
}
