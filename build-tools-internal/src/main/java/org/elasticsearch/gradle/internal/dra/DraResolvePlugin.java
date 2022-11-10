/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.dra;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.initialization.layout.BuildLayout;

import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static java.util.Map.Entry;

public class DraResolvePlugin implements Plugin<Project> {

    public static final String USE_DRA_ARTIFACTS_FLAG = "dra.artifacts";
    public static final String DRA_ARTIFACTS_DEPENDENCY_PREFIX = "dra.artifacts.dependency";
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
        boolean useDra = providerFactory.systemProperty(USE_DRA_ARTIFACTS_FLAG).map(Boolean::parseBoolean).getOrElse(false);
        project.getExtensions().getExtraProperties().set("useDra", useDra);
        if (useDra) {
            resolveBuildIdProperties().get().forEach((key, buildId) -> {
                configureDraRepository(
                    project,
                    "dra-snapshot-artifacts-" + key,
                    key,
                    buildId,
                    snapshotRepositoryPrefix.orElse("https://artifacts-snapshot.elastic.co/"),
                    ".*SNAPSHOT"
                );
                configureDraRepository(
                    project,
                    "dra-release-artifacts-" + key,
                    key,
                    buildId,
                    releaseRepositoryPrefix.orElse("https://artifacts.elastic.co/"),
                    "^(.(?!SNAPSHOT))*$"
                );
            });
        }
    }

    private void configureDraRepository(
        Project project,
        String repositoryName,
        String draKey,
        String buildId,
        Provider<String> repoPrefix,
        String includeVersionRegex
    ) {
        project.getRepositories().ivy(repo -> {
            repo.setName(repositoryName);
            repo.setUrl(repoPrefix.get());
            repo.patternLayout(patternLayout -> {
                patternLayout.artifact(
                    String.format("/%s/%s/downloads/%s/[module]-[revision]-[classifier].[ext]", draKey, buildId, draKey)
                );
                patternLayout.artifact(
                    String.format("/%s/%s/downloads/%s/[module]/[module]-[revision]-[classifier].[ext]", draKey, buildId, draKey)
                );
            });
            repo.metadataSources(metadataSources -> metadataSources.artifact());
            repo.content(repositoryContentDescriptor -> repositoryContentDescriptor.includeVersionByRegex(".*", ".*", includeVersionRegex));
        });
    }

    private Provider<Map<String, String>> resolveBuildIdProperties() {
        return providerFactory.systemPropertiesPrefixedBy(DRA_ARTIFACTS_DEPENDENCY_PREFIX)
            .map(
                stringStringMap -> stringStringMap.entrySet()
                    .stream()
                    .collect(
                        Collectors.toMap(entry -> entry.getKey().substring(DRA_ARTIFACTS_DEPENDENCY_PREFIX.length() + 1), Entry::getValue)
                    )
            );
    }
}
