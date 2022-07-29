/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.internal.ElasticsearchJavaBasePlugin;
import org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin;
import org.elasticsearch.gradle.internal.FixtureStop;
import org.elasticsearch.gradle.internal.InternalTestClustersPlugin;
import org.elasticsearch.gradle.internal.precommit.InternalPrecommitTasks;
import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.bundling.Zip;

import javax.inject.Inject;

import static org.elasticsearch.gradle.plugin.PluginBuildPlugin.BUNDLE_PLUGIN_TASK_NAME;
import static org.elasticsearch.gradle.plugin.PluginBuildPlugin.EXPLODED_BUNDLE_PLUGIN_TASK_NAME;

public class RestTestBasePlugin implements Plugin<Project> {
    private static final String TESTS_REST_CLUSTER = "tests.rest.cluster";
    private static final String TESTS_CLUSTER = "tests.cluster";
    private static final String TESTS_CLUSTER_NAME = "tests.clustername";
    private static final String TESTS_CLUSTER_READINESS = "tests.cluster.readiness";

    private ProviderFactory providerFactory;

    @Inject
    public RestTestBasePlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        project.getPluginManager().apply(ElasticsearchTestBasePlugin.class);
        project.getPluginManager().apply(InternalTestClustersPlugin.class);
        InternalPrecommitTasks.create(project, false);
        project.getTasks().withType(RestIntegTestTask.class).configureEach(restIntegTestTask -> {
            @SuppressWarnings("unchecked")
            NamedDomainObjectContainer<ElasticsearchCluster> testClusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
                .getExtensions()
                .getByName(TestClustersPlugin.EXTENSION_NAME);
            ElasticsearchCluster cluster = testClusters.maybeCreate(restIntegTestTask.getName());
            restIntegTestTask.useCluster(cluster);
            restIntegTestTask.include("**/*IT.class");
            restIntegTestTask.systemProperty("tests.rest.load_packaged", Boolean.FALSE.toString());
            if (systemProperty(TESTS_REST_CLUSTER) == null) {
                if (systemProperty(TESTS_CLUSTER) != null || systemProperty(TESTS_CLUSTER_NAME) != null) {
                    throw new IllegalArgumentException(
                        String.format("%s, %s, and %s must all be null or non-null", TESTS_REST_CLUSTER, TESTS_CLUSTER, TESTS_CLUSTER_NAME)
                    );
                }
                SystemPropertyCommandLineArgumentProvider runnerNonInputProperties =
                    (SystemPropertyCommandLineArgumentProvider) restIntegTestTask.getExtensions().getByName("nonInputProperties");
                runnerNonInputProperties.systemProperty(TESTS_REST_CLUSTER, () -> String.join(",", cluster.getAllHttpSocketURI()));
                runnerNonInputProperties.systemProperty(TESTS_CLUSTER, () -> String.join(",", cluster.getAllTransportPortURI()));
                runnerNonInputProperties.systemProperty(TESTS_CLUSTER_NAME, cluster::getName);
                runnerNonInputProperties.systemProperty(TESTS_CLUSTER_READINESS, () -> String.join(",", cluster.getAllReadinessPortURI()));
            } else {
                if (systemProperty(TESTS_CLUSTER) == null || systemProperty(TESTS_CLUSTER_NAME) == null) {
                    throw new IllegalArgumentException(
                        String.format("%s, %s, and %s must all be null or non-null", TESTS_REST_CLUSTER, TESTS_CLUSTER, TESTS_CLUSTER_NAME)
                    );
                }
            }
        });

        project.getTasks()
            .named(JavaBasePlugin.CHECK_TASK_NAME)
            .configure(check -> check.dependsOn(project.getTasks().withType(RestIntegTestTask.class)));
        project.getTasks()
            .withType(StandaloneRestIntegTestTask.class)
            .configureEach(t -> t.finalizedBy(project.getTasks().withType(FixtureStop.class)));

        project.getTasks().withType(StandaloneRestIntegTestTask.class).configureEach(t ->
        // if this a module or plugin, it may have an associated zip file with it's contents, add that to the test cluster
        project.getPluginManager().withPlugin("elasticsearch.esplugin", plugin -> {
            if (GradleUtils.isModuleProject(project.getPath())) {
                var bundle = project.getTasks().withType(Sync.class).named(EXPLODED_BUNDLE_PLUGIN_TASK_NAME);
                t.getClusters().forEach(c -> c.module(bundle));
            } else {
                var bundle = project.getTasks().withType(Zip.class).named(BUNDLE_PLUGIN_TASK_NAME);
                t.getClusters().forEach(c -> c.plugin(bundle));
            }
        }));
    }

    private String systemProperty(String propName) {
        return providerFactory.systemProperty(propName).getOrNull();
    }
}
