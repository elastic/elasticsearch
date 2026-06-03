/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Project;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

public abstract class ProjectSubscribeBuildService implements BuildService<BuildServiceParameters.None> {

    private final ProviderFactory providerFactory;

    /**
     * The filling of this map depends on the order of #registerProjectForTopic being called.
     * This is usually done during configuration phase, but we do not enforce yet the time of this method call.
     * The values are LinkedHashSet to preserve the order of registration mostly to provide a predicatable order
     * when running consecutive builds.
     * */
    private final Map<String, Collection<String>> versionsByTopic = new HashMap<>();

    @Inject
    public ProjectSubscribeBuildService(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    /**
     * Returning a provider so the evaluation of the map value is deferred to when the provider is queried.
     * */
    public Provider<Collection<String>> getProjectsByTopic(String topic) {
        return providerFactory.provider(() -> versionsByTopic.computeIfAbsent(topic, k -> new java.util.LinkedHashSet<>()));
    }

    public void registerProjectForTopic(String topic, Project project) {
        versionsByTopic.computeIfAbsent(topic, k -> new java.util.LinkedHashSet<>()).add(project.getPath());
    }
}
