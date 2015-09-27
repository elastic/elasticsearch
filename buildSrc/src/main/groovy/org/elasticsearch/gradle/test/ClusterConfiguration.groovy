package org.elasticsearch.gradle.test

import org.gradle.api.tasks.Input

/** Configuration for an elasticsearch cluster, used for integration tests. */
class ClusterConfiguration {

    @Input
    int numNodes = 1

    @Input
    int httpPort = 9400

    @Input
    int transportPort = 9500

    Map<String, String> systemProperties = new HashMap<>()

    @Input
    void systemProperty(String property, String value) {
        systemProperties.put(property, value)
    }

    LinkedHashMap<String, String[]> setupCommands = new LinkedHashMap<>()

    @Input
    void plugin(String name, File file) {
        setupCommands.put(name, ['bin/plugin', 'install', "file://${file}"])
    }

    @Input
    void setupCommand(String name, String[] args) {
        setupCommands.put(name, args)
    }
}
