package org.elasticsearch.gradle.test

import org.gradle.api.file.FileCollection
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

    LinkedHashMap<String, Object[]> setupCommands = new LinkedHashMap<>()

    @Input
    void plugin(String name, FileCollection file) {
        setupCommands.put(name, ['bin/plugin', 'install', new LazyFileUri(file: file)])
    }

    static class LazyFileUri {
        FileCollection file
        @Override
        String toString() {
            return "file://${file.singleFile}"
        }

    }

    @Input
    void setupCommand(String name, Object... args) {
        setupCommands.put(name, args)
    }
}
