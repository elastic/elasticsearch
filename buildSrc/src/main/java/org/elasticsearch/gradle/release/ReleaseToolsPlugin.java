/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.release;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class ReleaseToolsPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getTasks().register("generateReleaseNotes", GenerateReleaseNotesTask.class).configure(action -> {
            action.setGroup("Documentation");
            action.setDescription("Generates release notes from changelog files held in this checkout");
        });

        project.getTasks().register("validateChangelogs", ValidateChangelogsTask.class).configure(action -> {
            action.setGroup("Documentation");
            action.setDescription("Validates that all the changelogs YAML files are well-formed");
        });
    }
}
