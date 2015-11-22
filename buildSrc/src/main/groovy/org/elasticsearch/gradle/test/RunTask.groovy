package org.elasticsearch.gradle.test

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction

class RunTask extends DefaultTask {
    ClusterConfiguration clusterConfig = new ClusterConfiguration(httpPort: 9200, transportPort: 9300, daemonize: false)

    RunTask() {
        ClusterFormationTasks.setup(project, this, clusterConfig)
    }
}
