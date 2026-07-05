/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.crossproject;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectIdResolver;
import org.elasticsearch.transport.LinkedProjectConfig;
import org.elasticsearch.transport.LinkedProjectConfigService;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Helper method for maintaining a set of linked ProjectIds.
 * This splits into two implementations, one for single-project environments and one for multi-project environments.
 * Most Elasticsearch deployments will not support multi-project, so we do not have to maintain the
 * ConcurrentHashMap in those environments and can ignore the ProjectId entirely.
 */
public interface LinkedProjectsProvider {

    /**
     * @param projectId the origin project
     * @return the set of projects linked to the given origin ProjectId
     */
    Set<ProjectId> getLinkedProjects(ProjectId projectId);

    class Factory {
        private Factory() {}

        public static LinkedProjectsProvider create(
            ProjectIdResolver projectIdResolver,
            LinkedProjectConfigService linkedProjectConfigService
        ) {
            if (projectIdResolver.supportsMultipleProjects()) {
                return new MultiProjectLinkedProjectsProvider(linkedProjectConfigService);
            }
            return new SingleProjectLinkedProjectsProvider(linkedProjectConfigService);
        }
    }

    class MultiProjectLinkedProjectsProvider implements LinkedProjectsProvider, LinkedProjectConfigService.LinkedProjectConfigListener {

        private final ConcurrentHashMap<ProjectId, Set<ProjectId>> linkedProjectsByOrigin;

        MultiProjectLinkedProjectsProvider(LinkedProjectConfigService linkedProjectConfigService) {
            linkedProjectsByOrigin = new ConcurrentHashMap<>();
            for (LinkedProjectConfig config : linkedProjectConfigService.getInitialLinkedProjectConfigs()) {
                linkedProjectsByOrigin.computeIfAbsent(config.originProjectId(), k -> new CopyOnWriteArraySet<>())
                    .add(config.linkedProjectId());
            }
            linkedProjectConfigService.register(this);
        }

        @Override
        public Set<ProjectId> getLinkedProjects(ProjectId projectId) {
            var linkedProjects = linkedProjectsByOrigin.get(projectId);
            return linkedProjects == null || linkedProjects.isEmpty() ? Set.of() : Collections.unmodifiableSet(linkedProjects);
        }

        @Override
        public void updateLinkedProject(LinkedProjectConfig config) {
            linkedProjectsByOrigin.computeIfAbsent(config.originProjectId(), k -> new CopyOnWriteArraySet<>())
                .add(config.linkedProjectId());
        }

        @Override
        public void remove(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {
            Set<ProjectId> linked = linkedProjectsByOrigin.get(originProjectId);
            if (linked != null) {
                linked.remove(linkedProjectId);
            }
        }
    }

    class SingleProjectLinkedProjectsProvider implements LinkedProjectsProvider, LinkedProjectConfigService.LinkedProjectConfigListener {

        private final CopyOnWriteArraySet<ProjectId> linkedProjects;

        SingleProjectLinkedProjectsProvider(LinkedProjectConfigService linkedProjectConfigService) {
            linkedProjects = new CopyOnWriteArraySet<>(
                linkedProjectConfigService.getInitialLinkedProjectConfigs().stream().map(LinkedProjectConfig::linkedProjectId).toList()
            );
            linkedProjectConfigService.register(this);
        }

        @Override
        public Set<ProjectId> getLinkedProjects(ProjectId projectId) {
            return linkedProjects.isEmpty() ? Set.of() : Collections.unmodifiableSet(linkedProjects);
        }

        @Override
        public void updateLinkedProject(LinkedProjectConfig config) {
            linkedProjects.add(config.linkedProjectId());
        }

        @Override
        public void remove(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {
            linkedProjects.remove(linkedProjectId);
        }
    }
}
