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
import org.elasticsearch.gradle.transform.UnzipTransform;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.NamedDomainObjectProvider;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Zip;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.io.File;

import static org.elasticsearch.gradle.plugin.PluginBuildPlugin.BUNDLE_PLUGIN_TASK_NAME;
import static org.elasticsearch.gradle.plugin.PluginBuildPlugin.EXPLODED_BUNDLE_PLUGIN_TASK_NAME;

public class YamlRestTestPlugin implements Plugin<Project> {

    public static final String REST_TEST_SPECS_CONFIGURATION_NAME = "restTestSpecs";
    public static final String YAML_REST_TEST = "yamlRestTest";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(GradleTestPolicySetupPlugin.class);
        project.getPluginManager().apply(TestClustersPlugin.class);
        project.getPluginManager().apply(JavaBasePlugin.class);

        Attribute<Boolean> restAttribute = Attribute.of("restSpecs", Boolean.class);
        project.getDependencies().getAttributesSchema().attribute(restAttribute);
        project.getDependencies().getArtifactTypes().maybeCreate(ArtifactTypeDefinition.JAR_TYPE);
        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom()
                .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.JAR_TYPE)
                .attribute(restAttribute, true);
            transformSpec.getTo()
                .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
                .attribute(restAttribute, true);
        });

        ConfigurationContainer configurations = project.getConfigurations();
        Configuration restTestSpecs = configurations.create(REST_TEST_SPECS_CONFIGURATION_NAME);
        restTestSpecs.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
        restTestSpecs.getAttributes().attribute(restAttribute, true);

        TaskProvider<Copy> copyRestTestSpecs = project.getTasks().register("copyRestTestSpecs", Copy.class, t -> {
            t.from(restTestSpecs);
            t.into(new File(project.getBuildDir(), "restResources/restspec"));
        });

        var sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        var testSourceSet = sourceSets.maybeCreate(YAML_REST_TEST);
        NamedDomainObjectContainer<ElasticsearchCluster> testClusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
            .getExtensions()
            .getByName(TestClustersPlugin.EXTENSION_NAME);

        testSourceSet.getOutput().dir(copyRestTestSpecs.map(Task::getOutputs));
        Configuration yamlRestTestImplementation = configurations.getByName(testSourceSet.getImplementationConfigurationName());
        setupDefaultDependencies(project.getDependencies(), restTestSpecs, yamlRestTestImplementation);
        var cluster = testClusters.register(YAML_REST_TEST);
        TaskProvider<StandaloneRestIntegTestTask> yamlRestTestTask = setupTestTask(project, testSourceSet, cluster);
        project.getPlugins().withType(PluginBuildPlugin.class, p -> {
            if (GradleUtils.isModuleProject(project.getPath())) {
                var bundle = project.getTasks().withType(Sync.class).named(EXPLODED_BUNDLE_PLUGIN_TASK_NAME);
                cluster.configure(c -> c.module(bundle));
            } else {
                var bundle = project.getTasks().withType(Zip.class).named(BUNDLE_PLUGIN_TASK_NAME);
                cluster.configure(c -> c.plugin(bundle));
            }
        });

        // Wire up to check task
        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(yamlRestTestTask));
    }

    private static void setupDefaultDependencies(
        DependencyHandler dependencyHandler,
        Configuration restTestSpecs,
        Configuration yamlRestTestImplementation
    ) {
        String elasticsearchVersion = VersionProperties.getElasticsearch();
        yamlRestTestImplementation.defaultDependencies(
            deps -> deps.add(dependencyHandler.create("org.elasticsearch.test:yaml-rest-runner:" + elasticsearchVersion))
        );

        restTestSpecs.defaultDependencies(
            deps -> deps.add(dependencyHandler.create("org.elasticsearch:rest-api-spec:" + elasticsearchVersion))
        );
    }

    private TaskProvider<StandaloneRestIntegTestTask> setupTestTask(
        Project project,
        SourceSet testSourceSet,
        NamedDomainObjectProvider<ElasticsearchCluster> clusterProvider
    ) {
        return project.getTasks().register(YAML_REST_TEST, StandaloneRestIntegTestTask.class, task -> {
            task.useCluster(clusterProvider.get());
            task.setTestClassesDirs(testSourceSet.getOutput().getClassesDirs());
            task.setClasspath(testSourceSet.getRuntimeClasspath());

            var cluster = clusterProvider.get();
            var nonInputProperties = new SystemPropertyCommandLineArgumentProvider();
            nonInputProperties.systemProperty("tests.rest.cluster", () -> String.join(",", cluster.getAllHttpSocketURI()));
            nonInputProperties.systemProperty("tests.cluster", () -> String.join(",", cluster.getAllTransportPortURI()));
            nonInputProperties.systemProperty("tests.clustername", () -> cluster.getName());
            task.getJvmArgumentProviders().add(nonInputProperties);
            task.systemProperty("tests.rest.load_packaged", Boolean.FALSE.toString());
        });
    }

}
