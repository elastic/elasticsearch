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

package org.elasticsearch.gradle.test.rest;

import org.elasticsearch.gradle.ElasticsearchJavaPlugin;
import org.elasticsearch.gradle.test.RestIntegTestTask;
import org.elasticsearch.gradle.test.RestTestBasePlugin;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;

import static org.elasticsearch.gradle.test.rest.RestTestUtil.setupDependencies;
import static org.elasticsearch.gradle.test.rest.RestTestUtil.setupRunnerTask;
import static org.elasticsearch.gradle.test.rest.RestTestUtil.setupTask;

/**
 * Apply this plugin to run the YAML based REST tests.
 */
public class YamlRestTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "yamlRestTest";

    @Override
    public void apply(Project project) {

        project.getPluginManager().apply(ElasticsearchJavaPlugin.class);
        project.getPluginManager().apply(TestClustersPlugin.class);
        project.getPluginManager().apply(RestTestBasePlugin.class);
        project.getPluginManager().apply(RestResourcesPlugin.class);

        // create source set
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet yamlTestSourceSet = sourceSets.create(SOURCE_SET_NAME);

        // setup the yamlRestTest task
        RestIntegTestTask yamlRestTestTask = setupTask(project, SOURCE_SET_NAME);

        // setup the runner task
        setupRunnerTask(project, yamlRestTestTask, yamlTestSourceSet);

        // setup the dependencies
        setupDependencies(project, yamlTestSourceSet);

        // setup the copy for the rest resources
        project.getTasks().withType(CopyRestApiTask.class, copyRestApiTask -> {
            copyRestApiTask.sourceSetName = SOURCE_SET_NAME;
            project.getTasks().named(yamlTestSourceSet.getProcessResourcesTaskName()).configure(t -> t.dependsOn(copyRestApiTask));
        });
        project.getTasks().withType(CopyRestTestsTask.class, copyRestTestTask -> { copyRestTestTask.sourceSetName = SOURCE_SET_NAME; });

        // setup IDE
        GradleUtils.setupIdeForTestSourceSet(project, yamlTestSourceSet);

        // wire this task into check
        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(yamlRestTestTask));
    }
}
