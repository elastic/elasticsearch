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

package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

public class PrecommitTaskPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        TaskProvider<Task> precommit = project.getTasks().register(PrecommitPlugin.PRECOMMIT_TASK_NAME, t -> {
            t.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            t.setDescription("Runs all non-test checks");
        });

        project.getPluginManager()
            .withPlugin(
                "lifecycle-base",
                p -> project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(precommit))
            );
        project.getPluginManager()
            .withPlugin(
                "java",
                p -> {
                    // run compilation as part of precommit
                    for (SourceSet sourceSet : GradleUtils.getJavaSourceSets(project)) {
                        precommit.configure(t -> t.dependsOn(sourceSet.getClassesTaskName()));
                    }

                    // make sure tests run after all precommit tasks
                    project.getTasks().withType(Test.class).configureEach(t -> t.mustRunAfter(precommit));
                }
            );
    }
}
