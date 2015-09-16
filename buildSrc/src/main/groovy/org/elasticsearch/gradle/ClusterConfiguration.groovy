package org.elasticsearch.gradle

import org.gradle.api.tasks.Input
import org.gradle.util.ConfigureUtil

/** Configuration for an elasticsearch cluster, used for integration tests. */
class ClusterConfiguration {

    @Input
    int numNodes = 1

    @Input
    int httpPort = 9400

    @Input
    int transportPort = 9500

    ClusterSetupConfiguration setupConfig = new ClusterSetupConfiguration()

    @Input
    void setup(Closure closure) {
        ConfigureUtil.configure(closure, setupConfig)
    }

    // TODO: sys prop and/or command line args
}
