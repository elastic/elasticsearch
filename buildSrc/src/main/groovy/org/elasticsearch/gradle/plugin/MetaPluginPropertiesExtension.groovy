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
 * A container for meta plugin properties that will be written to the meta plugin descriptor, for easy
 * manipulation in the gradle DSL.
 */
class MetaPluginPropertiesExtension {
    @Input
    String name

    @Input
    String description

    /**
     * The plugins this meta plugin wraps.
     * Note this is not written to the plugin descriptor, but used to setup the final zip file task.
     */
    @Input
    List<String> plugins

    MetaPluginPropertiesExtension(Project project) {
        name = project.name
    }
}
