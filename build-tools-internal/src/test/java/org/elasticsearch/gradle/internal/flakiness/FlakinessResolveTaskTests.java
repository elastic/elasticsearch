/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link FlakinessResolveTask}'s task-specific pure logic (deriving the compile task list
 * from resolved base targets). The end-to-end behaviour - reading the {@link FlakinessModelService} at
 * execution time and writing the hand-off files - is exercised by {@code FlakinessResolvePluginFuncTest}.
 */
public class FlakinessResolveTaskTests {

    @Test
    public void testDistinctSortedCompileTasksExcludingBwc() {
        List<BaseTarget> targets = List.of(
            target(":server", "test", "org.foo.BTests", false, ":server:compileTestJava"),
            // Same compile task as above -> collapsed.
            target(":server", "test", "org.foo.ATests", false, ":server:compileTestJava"),
            target(":libs:x", "internalClusterTest", "org.foo.CIT", false, ":libs:x:compileInternalClusterTestJava"),
            // bwc target -> excluded (nothing to compile, it is skipped downstream).
            target(":qa:rolling", "javaRestTest", "org.foo.DIT", true, ":qa:rolling:compileJavaRestTestJava")
        );

        // Deterministic sorted order, deduped, bwc dropped.
        assertThat(
            FlakinessResolveTask.compileTaskPaths(targets),
            contains(":libs:x:compileInternalClusterTestJava", ":server:compileTestJava")
        );
    }

    @Test
    public void testNoCompileTasksWhenAllBwc() {
        List<BaseTarget> targets = List.of(target(":qa:a", "test", "org.foo.ATests", true, ":qa:a:compileTestJava"));
        assertThat(FlakinessResolveTask.compileTaskPaths(targets), is(empty()));
    }

    private static BaseTarget target(String project, String sourceSet, String fqcn, boolean bwc, String compileTaskPath) {
        return new BaseTarget(project, sourceSet, sourceSet, fqcn, null, null, bwc, compileTaskPath, "/out/" + project);
    }
}
