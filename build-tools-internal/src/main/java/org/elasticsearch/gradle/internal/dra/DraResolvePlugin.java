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

import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static java.util.Map.Entry;

public class DraResolvePlugin implements Plugin<Project> {

    public static final String USE_DRA_ARTIFACTS_FLAG = "dra.artifacts";
    public static final String DRA_WORKFLOW = "dra.workflow";
    public static final String DRA_ARTIFACTS_DEPENDENCY_PREFIX = "dra.artifacts.dependency";
    private final ProviderFactory providerFactory;

    private final Provider<String> repositoryPrefix;

    @Inject
    public DraResolvePlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
        this.repositoryPrefix = providerFactory.systemProperty("dra.artifacts.url.repo.prefix");
    }

    @Override
    public void apply(Project project) {
        boolean useDra = providerFactory.systemProperty(USE_DRA_ARTIFACTS_FLAG).map(Boolean::parseBoolean).getOrElse(false);
        project.getExtensions().getExtraProperties().set("useDra", useDra);
        if (useDra) {
            DraWorkflow workflow = providerFactory.systemProperty(DRA_WORKFLOW).map(String::toUpperCase).map(DraWorkflow::valueOf).get();
            resolveBuildIdProperties().get().forEach((key, buildId) -> {
                configureDraRepository(
                    project,
                    "dra-" + workflow.name().toLowerCase() + "-artifacts-" + key,
                    key,
                    buildId,
                    repositoryPrefix.orElse(workflow.repository),
                    workflow.versionRegex
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
                patternLayout.artifact(String.format("/%s/%s/downloads/%s/[module]-[revision].[ext]", draKey, buildId, draKey));
                patternLayout.artifact(String.format("/%s/%s/downloads/%s/[module]/[module]-[revision].[ext]", draKey, buildId, draKey));
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

    enum DraWorkflow {
        SNAPSHOT("https://artifacts-snapshot.elastic.co/", ".*SNAPSHOT"),
        STAGING("https://artifacts-staging.elastic.co/", "^(.(?!SNAPSHOT))*$"),
        RELEASE("https://artifacts.elastic.co/", "^(.(?!SNAPSHOT))*$");

        private final String repository;
        public String versionRegex;

        DraWorkflow(String repository, String versionRegex) {
            this.repository = repository;
            this.versionRegex = versionRegex;
        }
    }
}
