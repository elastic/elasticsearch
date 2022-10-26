/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.dra;

import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.initialization.layout.BuildLayout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

import javax.inject.Inject;

public class DraResolvePlugin implements Plugin<Project> {

    private final ProviderFactory providerFactory;
    private BuildLayout buildLayout;

    private final Provider<String> snapshotRepositoryPrefix;
    private final Provider<String> releaseRepositoryPrefix;

    @Inject
    public DraResolvePlugin(ProviderFactory providerFactory, BuildLayout buildLayout) {
        this.providerFactory = providerFactory;
        this.buildLayout = buildLayout;
        this.snapshotRepositoryPrefix = providerFactory.systemProperty("dra.artifacts.url.repo.snapshot.prefix");
        this.releaseRepositoryPrefix = providerFactory.systemProperty("dra.artifacts.url.repo.release.prefix");
    }

    @Override
    public void apply(Project project) {
        if (providerFactory.systemProperty("dra.artifacts").isPresent()) {
            Properties buildIdProperties = resolveBuildIdProperties();
            buildIdProperties.forEach((key, buildId) -> {
                configureDraRepository(project, "dra-snapshot-artifacts-" + key,key.toString(), buildId.toString(), snapshotRepositoryPrefix.orElse("https://artifacts-snapshot.elastic.co/"), ".*SNAPSHOT");
                configureDraRepository(project, "dra-release-artifacts-" + key, key.toString(), buildId.toString(), releaseRepositoryPrefix.orElse("https://artifacts.elastic.co/"), "^(.(?!SNAPSHOT))*$");
            });
        }
    }

    private void configureDraRepository(Project project, String repositoryName, String draKey, String buildId, Provider<String> repoPrefix,  String includeVersionRegex) {
        project.getRepositories().ivy(repo -> {
            repo.setName(repositoryName);
            repo.setUrl(repoPrefix.get());
            repo.patternLayout( patternLayout -> {
                patternLayout.artifact(String.format("/%s/%s/downloads/%s/[module]/[module]-[revision]-[classifier].[ext]", draKey, buildId, draKey));
                patternLayout.artifact(String.format("/%s/%s/downloads/%s/[module]-[revision]-[classifier].[ext]", draKey, buildId, draKey));
            });
            repo.metadataSources(metadataSources -> metadataSources.artifact());
            repo.content(
                repositoryContentDescriptor -> repositoryContentDescriptor.includeVersionByRegex(".*", ".*", includeVersionRegex)
            );
        });
    }

    private Properties resolveBuildIdProperties() {
        Properties draProperties = new Properties();
        try {
            draProperties.load(Files.newInputStream(new File(buildLayout.getRootDirectory(), "buildIds.properties").toPath()));
        } catch (IOException e) {
            throw new GradleException("Cannot resolve buildIds properties file", e);
        }
        return draProperties;
    }
}
