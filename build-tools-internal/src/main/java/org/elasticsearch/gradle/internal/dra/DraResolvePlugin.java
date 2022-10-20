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

    @Inject
    public DraResolvePlugin(ProviderFactory providerFactory, BuildLayout buildLayout) {
        this.providerFactory = providerFactory;
        this.buildLayout = buildLayout;
    }

    @Override
    public void apply(Project project) {
        if (providerFactory.systemProperty("dra.artifacts").isPresent()) {
            Properties buildIdProperties = resolveBuildIdProperties();
            buildIdProperties.forEach((key, buildId) -> {
                project.getRepositories().ivy(repo -> {
                    repo.setName("dra-artifacts-" + key.toString());
                    // TODO handle releases
                    repo.setUrl("https://artifacts-snapshot.elastic.co/");
                    repo.patternLayout(patternLayout -> patternLayout.artifact(calculateArtifactPattern(key, buildId)));
                    repo.metadataSources(metadataSources -> metadataSources.artifact());

                    // TODO handle content filtering
                    // repo.content(repositoryContentDescriptor -> repositoryContentDescriptor.includeGroup(key.toString()));
                });
            });
        }
    }

    private static String calculateArtifactPattern(Object key, Object buildId) {
        return String.format("/%s/%s/downloads/%s/[module]-[revision]-[classifier].[ext]", key, buildId, key);
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
