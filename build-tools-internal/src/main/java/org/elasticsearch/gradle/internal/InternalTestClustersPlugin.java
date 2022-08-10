/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.ProviderFactory;

import java.util.Random;

import javax.inject.Inject;

public class InternalTestClustersPlugin implements Plugin<Project> {

    private ProviderFactory providerFactory;

    @Inject
    public InternalTestClustersPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(InternalDistributionDownloadPlugin.class);
        project.getRootProject().getPluginManager().apply(InternalReaperPlugin.class);
        TestClustersPlugin testClustersPlugin = project.getPlugins().apply(TestClustersPlugin.class);
        testClustersPlugin.setRuntimeJava(providerFactory.provider(() -> BuildParams.getRuntimeJavaHome()));
        testClustersPlugin.setIsReleasedVersion(
            version -> (version.equals(VersionProperties.getElasticsearchVersion()) && BuildParams.isSnapshotBuild() == false)
                || BuildParams.getBwcVersions().unreleasedInfo(version) == null
        );
        NamedDomainObjectContainer<ElasticsearchCluster> testClusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
            .getExtensions()
            .getByName(TestClustersPlugin.EXTENSION_NAME);
        setRandomNodeProcessors(testClusters);
    }

    private void setRandomNodeProcessors(NamedDomainObjectContainer<ElasticsearchCluster> testClusters) {
        final var random = new Random(Long.parseUnsignedLong(BuildParams.getTestSeed(), 16));
        // Set node.processors to 1 rarely
        if (random.nextInt(100) >= 90) {
            testClusters.configureEach(elasticsearchCluster -> elasticsearchCluster.setting("node.processors", "1"));
        }
    }
}
