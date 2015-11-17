package org.elasticsearch.gradle.test

import org.gradle.api.DefaultTask
import org.gradle.api.Project

class RunTask extends DefaultTask {

    ClusterConfiguration clusterConfig = new ClusterConfiguration(httpPort: 9200, transportPort: 9300, daemonize: false)

    RunTask() {
        project.afterEvaluate {
            ClusterFormationTasks.setup(project, this, clusterConfig)
        }
    }

    static void configure(Project project) {
        RunTask task = project.tasks.create(
            name: 'run',
            type: RunTask,
            description: "Runs elasticsearch with '${project.path}'",
            group: 'Verification')
    }
}
