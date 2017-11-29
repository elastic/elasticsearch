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
package org.elasticsearch.gradle.plugin

import org.gradle.api.Project
import org.gradle.api.tasks.Input

/**
 * A container for plugin properties that will be written to the plugin descriptor, for easy
 * manipulation in the gradle DSL.
 */
class PluginPropertiesExtension {

    @Input
    String name

    @Input
    String version

    @Input
    String description

    @Input
    String classname

    @Input
    boolean hasNativeController = false

    /** Indicates whether the plugin jar should be made available for the transport client. */
    @Input
    boolean hasClientJar = false

    /** True if the plugin requires the elasticsearch keystore to exist, false otherwise. */
    @Input
    boolean requiresKeystore = false

    /** A license file that should be included in the built plugin zip. */
    @Input
    File licenseFile = null

    /**
     * A notice file that should be included in the built plugin zip. This will be
     * extended with notices from the {@code licenses/} directory.
     */
    @Input
    File noticeFile = null

    PluginPropertiesExtension(Project project) {
        name = project.name
        version = project.version
    }
}
