package org.elasticsearch.gradle.test

import com.carrotsearch.gradle.randomizedtesting.RandomizedTestingTask
import org.elasticsearch.gradle.BuildPlugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.Input
import org.gradle.util.ConfigureUtil

/**
 * Runs integration tests, but first starts an ES cluster,
 * and passes the ES cluster info as parameters to the tests.
 */
class RestIntegTestTask extends RandomizedTestingTask {

    ClusterConfiguration clusterConfig = new ClusterConfiguration()

    @Input
    boolean includePackaged = false

    static RestIntegTestTask configure(Project project) {
        Map integTestOptions = [
            name: 'integTest',
            type: RestIntegTestTask,
            dependsOn: 'testClasses',
            group: JavaBasePlugin.VERIFICATION_GROUP,
            description: 'Runs rest tests against an elasticsearch cluster.'
        ]
        RestIntegTestTask integTest = project.tasks.create(integTestOptions)
        integTest.configure(BuildPlugin.commonTestConfig(project))
        integTest.configure {
            include '**/*IT.class'
            sysProp 'tests.rest.load_packaged', 'false'
        }
        RandomizedTestingTask test = project.tasks.findByName('test')
        if (test != null) {
            integTest.classpath = test.classpath
            integTest.testClassesDir = test.testClassesDir
            integTest.mustRunAfter(test)
        }
        project.check.dependsOn(integTest)
        RestSpecHack.configureDependencies(project)
        project.afterEvaluate {
            integTest.dependsOn(RestSpecHack.configureTask(project, integTest.includePackaged))
        }
        return integTest
    }

    RestIntegTestTask() {
        project.afterEvaluate {
            Task test = project.tasks.findByName('test')
            if (test != null) {
                mustRunAfter(test)
            }
            ClusterFormationTasks.setup(project, this, clusterConfig)
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
