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
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class ProjectSubscribeBuildService implements BuildService<BuildServiceParameters.None> {

    public static final String TRANSPORT_REFERENCES_TOPIC = "transportReferences";

    private Map<String, Collection<String>> versionsByTopic = new HashMap<>();

    public Collection<String> getProjectsByTopic(String topic) {
        return versionsByTopic.get(topic);
    }

    public void registerProjectForTopic(String topic, Project project) {
        versionsByTopic.computeIfAbsent(topic, k -> new java.util.LinkedHashSet<>()).add(project.getPath());
    }
}
