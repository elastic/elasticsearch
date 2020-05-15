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

import org.elasticsearch.gradle.plugin.PluginPropertiesExtension;
import org.elasticsearch.gradle.test.RestIntegTestTask;
import org.elasticsearch.gradle.testclusters.RestTestRunnerTask;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.bundling.Zip;
import org.gradle.plugins.ide.eclipse.model.EclipseModel;
import org.gradle.plugins.ide.idea.model.IdeaModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Apply this plugin to run the YAML based REST tests. This will adda
 */
public class YamlTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "yamlTest";

    @Override
    public void apply(Project project) {

        if (project.getPluginManager().hasPlugin("elasticsearch.build") == false
            && project.getPluginManager().hasPlugin("elasticsearch.standalone-rest-test") == false) {
            throw new InvalidUserDataException(
                "elasticsearch.build or elasticsearch.standalone-rest-test plugin " + "must be applied before the YAML test plugin"
            );
        }

        // project.getPluginManager().apply(JavaPlugin.class); // for the Java based runner
        project.getPluginManager().apply(TestClustersPlugin.class); // to spin up the external cluster
        project.getPluginManager().apply(RestResourcesPlugin.class); // to copy around the yaml tests and json spec

        // create source set
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet yamlTestSourceSet = sourceSets.create("yamlTest");

        // create task
        RestIntegTestTask yamlTestTask = project.getTasks()
            .create("yamlTest", RestIntegTestTask.class, task -> {
                task.dependsOn(project.getTasks().getByName("copyRestApiSpecsTask"));
            });
        yamlTestTask.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
        yamlTestTask.setDescription("Runs the YAML based REST tests against an external cluster");

        // setup task dependency
        project.getDependencies().add("yamlTestCompile", project.project(":test:framework"));

        // ensure correct dependency and execution order
        project.getTasks().getByName("check").dependsOn(yamlTestTask);
        Task testTask = project.getTasks().findByName("test");
        if (testTask != null) {
            yamlTestTask.mustRunAfter(testTask);
        }
        yamlTestTask.mustRunAfter(project.getTasks().getByName("precommit"));

        // setup the runner
        RestTestRunnerTask runner = (RestTestRunnerTask) project.getTasks().getByName(yamlTestTask.getName() + "Runner");
        runner.setTestClassesDirs(yamlTestSourceSet.getOutput().getClassesDirs());
        runner.setClasspath(yamlTestSourceSet.getRuntimeClasspath());

        // if this a module or plugin, it may have an associated zip file with it's contents, add that to the test cluster
        boolean isModule = project.getPath().startsWith(":modules:");
        Zip bundle = (Zip) project.getTasks().findByName("bundlePlugin");
        if (bundle != null) {
            yamlTestTask.dependsOn(bundle);
            if (isModule) {
                runner.getClusters().forEach(c -> c.module(bundle.getArchiveFile()));
            } else {
                runner.getClusters().forEach(c -> c.plugin(project.getObjects().fileProperty().value(bundle.getArchiveFile())));
            }
        }

        // es-plugins may declare dependencies on additional modules, add those to the test cluster too.
        project.afterEvaluate(p -> {
            PluginPropertiesExtension pluginPropertiesExtension = project.getExtensions().findByType(PluginPropertiesExtension.class);
            if (pluginPropertiesExtension != null) { // not all projects are defined as plugins
                pluginPropertiesExtension.getExtendedPlugins().forEach(pluginName -> {
                    Project extensionProject = project.getProject().findProject(":modules:" + pluginName);
                    if (extensionProject != null) { // extension plugin may be defined, but not required to be a module
                        Zip extensionBundle = (Zip) extensionProject.getTasks().getByName("bundlePlugin");
                        yamlTestTask.dependsOn(extensionBundle);
                        runner.getClusters().forEach(c -> c.module(extensionBundle.getArchiveFile()));
                    }
                });
            }
        });

        // setup eclipse
        EclipseModel eclipse = project.getExtensions().getByType(EclipseModel.class);
        List<SourceSet> eclipseSourceSets = StreamSupport.stream(eclipse.getClasspath().getSourceSets().spliterator(), false)
            .collect(Collectors.toList());
        eclipseSourceSets.add(yamlTestSourceSet);
        eclipse.getClasspath().setSourceSets(eclipseSourceSets);
        List<Configuration> plusConfiguration = new ArrayList<>(eclipse.getClasspath().getPlusConfigurations());
        plusConfiguration.add(project.getConfigurations().getByName("yamlTestRuntimeClasspath"));
        eclipse.getClasspath().setPlusConfigurations(plusConfiguration);

        // setup intellij
        IdeaModel idea = project.getExtensions().getByType(IdeaModel.class);
        idea.getModule().getTestSourceDirs().addAll(yamlTestSourceSet.getJava().getSrcDirs());
        idea.getModule()
            .getScopes()
            .put("TEST", Map.of("plus", Collections.singletonList(project.getConfigurations().getByName("yamlTestRuntimeClasspath"))));
    }
}
