package org.elasticsearch.gradle.test

import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.api.internal.tasks.options.Option

class RunTask extends DefaultTask {

    ClusterConfiguration clusterConfig = new ClusterConfiguration(httpPort: 9200, transportPort: 9300, daemonize: false)

    RunTask() {
        project.afterEvaluate {
            ClusterFormationTasks.setup(project, this, clusterConfig)
        }
    }

    @Option(
        option = "debug-jvm",
        description = "Enable debugging configuration, to allow attaching a debugger to elasticsearch."
    )
    public void setDebug(boolean enabled) {
        clusterConfig.debug = enabled;
    }

    static void configure(Project project) {
        RunTask task = project.tasks.create(
            name: 'run',
            type: RunTask,
            description: "Runs elasticsearch with '${project.path}'",
            group: 'Verification')
    }
}
