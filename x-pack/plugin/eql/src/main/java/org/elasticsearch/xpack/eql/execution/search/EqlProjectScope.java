/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.search.crossproject.ProjectRoutingInfo;
import org.elasticsearch.search.crossproject.TargetProjects;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;

import java.util.stream.Collectors;

/** Rejects routing changes without replacing the projects independently resolved by authorization. */
public final class EqlProjectScope {
    private EqlProjectScope() {}

    /** A null expected scope denotes execution without a cross-project authorization filter. */
    public static void ensureSame(TargetProjects expected, TargetProjects actual) {
        if (expected == null) {
            return;
        }
        if (actual == null
            || expected.allProjects()
                .collect(Collectors.toMap(ProjectRoutingInfo::projectAlias, ProjectRoutingInfo::projectId))
                .equals(
                    actual.allProjects().collect(Collectors.toMap(ProjectRoutingInfo::projectAlias, ProjectRoutingInfo::projectId))
                ) == false) {
            throw new EqlIllegalArgumentException("project routing changed during EQL execution; retry the query");
        }
    }
}
