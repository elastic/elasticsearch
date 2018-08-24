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

package org.elasticsearch.gradle;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.ExtraPropertiesExtension;

import java.util.List;

/**
 * Sets {@code project.isEclipse} and @{code project.isIdea}. They are set to
 * true if the build is running in that IDE or false if it isn't.
 */
public class IsIdePlugin implements Plugin<Project> {
    public void apply(Project project) {
        ExtraPropertiesExtension ext = project.getExtensions().getByType(ExtraPropertiesExtension.class);
        List<String> startTasks = project.getGradle().getStartParameter().getTaskNames();
        /*
         * Note: This ******must****** line up with the logic in
         * settings.gradle or Eclipse users will have a bad time.
         */
        ext.set("isEclipse",
                   System.getProperty("eclipse.launcher") != null    // Detects gradle launched from the Eclipse IDE
                || System.getProperty("eclipse.application") != null // Detects gradle launched from the Eclipse compiler server
                || startTasks.contains("eclipse")                    // Detects gradle launched from the command line to do Eclipse stuff
                || startTasks.contains("cleanEclipse"));
        ext.set("isIdea",
                   System.getProperty("idea.active") != null         // Detects gradle launched from the IntelliJ
                || startTasks.contains("idea")                       // Detects gradle launched from the command line to do IntelliJ stuff
                || startTasks.contains("cleanIdea"));
    }
}
