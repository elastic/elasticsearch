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
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Zip;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import static org.elasticsearch.gradle.plugin.BasePluginBuildPlugin.BUNDLE_PLUGIN_TASK_NAME;

public class JavaRestTestPlugin implements Plugin<Project> {

    public static final String JAVA_REST_TEST = "javaRestTest";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(GradleTestPolicySetupPlugin.class);
        project.getPluginManager().apply(TestClustersPlugin.class);
        project.getPluginManager().apply(JavaBasePlugin.class);

        // Setup source set and dependencies
        var sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        var testSourceSet = sourceSets.maybeCreate(JAVA_REST_TEST);
        var javaRestTestImplementation = project.getConfigurations().getByName(testSourceSet.getImplementationConfigurationName());

        String elasticsearchVersion = VersionProperties.getElasticsearch();
        javaRestTestImplementation.defaultDependencies(
            deps -> deps.add(project.getDependencies().create("org.elasticsearch.test:framework:" + elasticsearchVersion))
        );

        // Register test cluster
        NamedDomainObjectContainer<ElasticsearchCluster> testClusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
            .getExtensions()
            .getByName(TestClustersPlugin.EXTENSION_NAME);
        var clusterProvider = testClusters.register(JAVA_REST_TEST);

        // Register test task
        TaskProvider<StandaloneRestIntegTestTask> javaRestTestTask = project.getTasks()
            .register(JAVA_REST_TEST, StandaloneRestIntegTestTask.class, task -> {
                task.useCluster(clusterProvider);
                task.setTestClassesDirs(testSourceSet.getOutput().getClassesDirs());
                task.setClasspath(testSourceSet.getRuntimeClasspath());

                var cluster = clusterProvider.get();
                var nonInputProperties = new SystemPropertyCommandLineArgumentProvider();
                nonInputProperties.systemProperty("tests.rest.cluster", () -> String.join(",", cluster.getAllHttpSocketURI()));
                nonInputProperties.systemProperty("tests.cluster", () -> String.join(",", cluster.getAllTransportPortURI()));
                nonInputProperties.systemProperty("tests.clustername", () -> cluster.getName());
                nonInputProperties.systemProperty("tests.cluster.readiness", () -> String.join(",", cluster.getAllReadinessPortURI()));
                task.getJvmArgumentProviders().add(nonInputProperties);
            });

        // Register plugin bundle with test cluster
        project.getPlugins().withType(PluginBuildPlugin.class, p -> {
            TaskProvider<Zip> bundle = project.getTasks().withType(Zip.class).named(BUNDLE_PLUGIN_TASK_NAME);
            clusterProvider.configure(c -> c.plugin(bundle.flatMap(Zip::getArchiveFile)));
            javaRestTestTask.configure(t -> t.dependsOn(bundle));
        });

        // Wire up to check task
        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(javaRestTestTask));
    }
}
