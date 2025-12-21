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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Holds information about the target projects for a cross-project search request. This record is used both by the
 * project authorization filter and project routing logic.
 * @param originProject the origin project, can be null if the request is not cross-project OR it was excluded by
 *                      project routing
 * @param linkedProjects all projects that are linked and authorized, can be empty if the request is not cross-project
 */
public record TargetProjects(
    @Nullable ProjectRoutingInfo originProject, // null when CPS is disabled or the local project is excluded by routing
    @Nullable List<ProjectRoutingInfo> linkedProjects // null when CPS is disabled
) {
    // Constant for representing no target project at all. Note this has a non-null empty linkedProjects field.
    public static final TargetProjects EMPTY = new TargetProjects(null, List.of());

    // A placeholder constant to satisfy the AuthorizedProjectResolver contract when CPS is disabled. The field values
    // are chosen so that it is a combination that is impossible to have when CPS is enabled. Hence, they are not
    // meant to be always interpreted literally, i.e. the `null` originProject does NOT mean the local project is excluded.
    // Instead, actions are always and only executed against the local project when CPS is disabled. =
    // This is different the above EMPTY field and its isEmpty check returns `false`.
    public static final TargetProjects LOCAL_ONLY_FOR_CPS_DISABLED = new TargetProjects(null, null);

    static {
        assert EMPTY.isEmpty();
        assert EMPTY.crossProject() == false;
        assert LOCAL_ONLY_FOR_CPS_DISABLED.isEmpty() == false;
        assert LOCAL_ONLY_FOR_CPS_DISABLED.crossProject() == false;
    }

    public TargetProjects(ProjectRoutingInfo originProject) {
        this(originProject, List.of());
    }

    @Nullable
    public String originProjectAlias() {
        return originProject != null ? originProject.projectAlias() : null;
    }

    public Set<String> allProjectAliases() {
        // TODO consider caching this
        return allProjects().map(ProjectRoutingInfo::projectAlias).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Stream containing all project infos, including origin and linked projects
     */
    public Stream<ProjectRoutingInfo> allProjects() {
        return Stream.concat(
            originProject != null ? Stream.of(originProject) : Stream.empty(),
            linkedProjects != null ? linkedProjects.stream() : Stream.empty()
        );
    }

    // TODO: Either change the definition or the method name since it allows targeting only the origin project without any remotes
    public boolean crossProject() {
        return originProject != null || (linkedProjects != null && linkedProjects.isEmpty() == false);
    }

    public boolean isEmpty() {
        return originProject == null && (linkedProjects != null && linkedProjects.isEmpty());
    }
}
