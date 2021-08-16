/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.plugin.PluginBuildPlugin;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Zip;

import java.io.File;

import static org.elasticsearch.gradle.plugin.PluginBuildPlugin.BUNDLE_PLUGIN_TASK_NAME;

public class YamlRestTestPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(GradleTestPolicySetupPlugin.class);
        project.getPluginManager().apply(TestClustersPlugin.class);
        project.getPluginManager().apply(JavaBasePlugin.class);

        ConfigurationContainer configurations = project.getConfigurations();
        Configuration restTestSpecs = configurations.create("restTestSpecs");
        TaskProvider<Copy> copyRestTestSpecs = project.getTasks().register("copyRestTestSpecs", Copy.class, t -> {
            t.from(project.zipTree(restTestSpecs.getSingleFile()));
            t.into(new File(project.getBuildDir(), "restResources/restspec"));
        });

        var sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        var testSourceSet = sourceSets.maybeCreate("yamlRestTest");
        NamedDomainObjectContainer<ElasticsearchCluster> testClusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
            .getExtensions()
            .getByName(TestClustersPlugin.EXTENSION_NAME);

        var cluster = testClusters.maybeCreate("yamlRestTest");
        testSourceSet.getOutput().dir(copyRestTestSpecs.map(Task::getOutputs));

        String elasticsearchVersion = VersionProperties.getElasticsearch();
        Configuration yamlRestTestImplementation = configurations.getByName(testSourceSet.getImplementationConfigurationName());
        System.out.println("yamlRestTestImplementation.getName() = " + yamlRestTestImplementation.getName());

        yamlRestTestImplementation.defaultDependencies(
            deps -> deps.add(project.getDependencies().create("org.elasticsearch.test:framework:" + elasticsearchVersion))
        );

        restTestSpecs.defaultDependencies(
            deps -> deps.add(project.getDependencies().create("org.elasticsearch:rest-api-spec:" + elasticsearchVersion))
        );

        var yamlRestTestTask = project.getTasks().register("yamlRestTest", StandaloneRestIntegTestTask.class, task -> {
            task.useCluster(cluster);
            task.setTestClassesDirs(testSourceSet.getOutput().getClassesDirs());
            task.setClasspath(testSourceSet.getRuntimeClasspath());

            var nonInputProperties = new SystemPropertyCommandLineArgumentProvider();
            nonInputProperties.systemProperty("tests.rest.cluster", () -> String.join(",", cluster.getAllHttpSocketURI()));
            nonInputProperties.systemProperty("tests.cluster", () -> String.join(",", cluster.getAllTransportPortURI()));
            nonInputProperties.systemProperty("tests.clustername", () -> cluster.getName());
            task.getJvmArgumentProviders().add(nonInputProperties);
            task.systemProperty("tests.rest.load_packaged", Boolean.FALSE.toString());
        });

        project.getPlugins().withType(PluginBuildPlugin.class, p -> {
            TaskProvider<Zip> bundle = project.getTasks().withType(Zip.class).named(BUNDLE_PLUGIN_TASK_NAME);
            cluster.plugin(bundle.flatMap(Zip::getArchiveFile));
            yamlRestTestTask.configure(t -> { t.dependsOn(bundle); });
        });
    }

}
