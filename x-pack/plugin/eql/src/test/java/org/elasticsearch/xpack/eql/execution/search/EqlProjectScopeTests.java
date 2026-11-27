/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.search.crossproject.ProjectRoutingInfo;
import org.elasticsearch.search.crossproject.TargetProjects;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;

import java.util.List;

public class EqlProjectScopeTests extends ESTestCase {
    public void testRejectsChangedProjectsAndAliasRetargeting() {
        TargetProjects expected = scope("a", "id-a");
        expectThrows(EqlIllegalArgumentException.class, () -> EqlProjectScope.ensureSame(expected, scope("b", "id-b")));
        expectThrows(EqlIllegalArgumentException.class, () -> EqlProjectScope.ensureSame(expected, scope("a", "id-b")));
        expectThrows(EqlIllegalArgumentException.class, () -> EqlProjectScope.ensureSame(expected, TargetProjects.EMPTY));
        expectThrows(EqlIllegalArgumentException.class, () -> EqlProjectScope.ensureSame(expected, null));
    }

    public void testAcceptsSameScopeDespiteMetadataChanges() {
        TargetProjects expected = scope("a", "id-a");
        TargetProjects actual = new TargetProjects(null, expected.linkedProjects(), null, true);
        EqlProjectScope.ensureSame(expected, actual);
        EqlProjectScope.ensureSame(null, null);
    }

    static TargetProjects scope(String alias, String id) {
        return new TargetProjects(null, List.of(new ProjectRoutingInfo(ProjectId.fromId(id), "elasticsearch", alias, "org", null)));
    }
}
