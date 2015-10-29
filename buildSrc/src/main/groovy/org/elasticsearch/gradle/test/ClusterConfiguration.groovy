/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
            return file.singleFile.toURI().toURL().toString();
        }

    }

    @Input
    void setupCommand(String name, Object... args) {
        setupCommands.put(name, args)
    }
}
