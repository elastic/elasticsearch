/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.core.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Holds information about the target projects for a cross-project search request. This record is used both by the
 * project authorization filter and project routing logic.
 * @param originProject the origin project, can be null if the request is not cross-project OR it was excluded by
 *                      project routing
 * @param linkedProjects all projects that are linked and authorized, can be empty if the request is not cross-project
 */
public record TargetProjects(@Nullable ProjectRoutingInfo originProject, List<ProjectRoutingInfo> linkedProjects) {
    public static final TargetProjects NOT_CROSS_PROJECT = new TargetProjects(null, List.of());

    public TargetProjects(ProjectRoutingInfo originProject) {
        this(originProject, List.of());
    }

    @Nullable
    public String originProjectAlias() {
        return originProject != null ? originProject.projectAlias() : null;
    }

    public Set<String> allProjectAliases() {
        // TODO consider caching this
        final Set<String> allProjectAliases = linkedProjects.stream().map(ProjectRoutingInfo::projectAlias).collect(Collectors.toSet());
        if (originProject != null) {
            allProjectAliases.add(originProject.projectAlias());
        }
        return Collections.unmodifiableSet(allProjectAliases);
    }

    public boolean crossProject() {
        return originProject != null || linkedProjects.isEmpty() == false;
    }
}
