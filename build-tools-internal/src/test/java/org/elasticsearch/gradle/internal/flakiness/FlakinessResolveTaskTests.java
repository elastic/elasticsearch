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
    public void testDistinctSortedCompileTasksExcludingSkipped() {
        List<BaseTarget> targets = List.of(
            runnable(":server", "test", "org.foo.BTests", ":server:compileTestJava"),
            // Same compile task as above -> collapsed.
            runnable(":server", "test", "org.foo.ATests", ":server:compileTestJava"),
            runnable(":libs:x", "internalClusterTest", "org.foo.CIT", ":libs:x:compileInternalClusterTestJava"),
            // A bwc target IS compiled now: its v<version>#bwcTest tasks really run those classes.
            runnable(":qa:rolling", "javaRestTest", "org.foo.DIT", ":qa:rolling:compileJavaRestTestJava"),
            // Nothing can run this one -> nothing to compile for it.
            skipped(":qa:packaging", "test", "org.foo.EIT", ":qa:packaging:compileTestJava")
        );

        // Deterministic sorted order, deduped, unrunnable dropped.
        assertThat(
            FlakinessResolveTask.compileTaskPaths(targets),
            contains(":libs:x:compileInternalClusterTestJava", ":qa:rolling:compileJavaRestTestJava", ":server:compileTestJava")
        );
    }

    @Test
    public void testNoCompileTasksWhenNothingIsRunnable() {
        List<BaseTarget> targets = List.of(skipped(":qa:a", "test", "org.foo.ATests", ":qa:a:compileTestJava"));
        assertThat(FlakinessResolveTask.compileTaskPaths(targets), is(empty()));
    }

    private static BaseTarget runnable(String project, String sourceSet, String fqcn, String compileTaskPath) {
        return target(project, sourceSet, fqcn, compileTaskPath, List.of(project + ":" + sourceSet), null);
    }

    private static BaseTarget skipped(String project, String sourceSet, String fqcn, String compileTaskPath) {
        return target(project, sourceSet, fqcn, compileTaskPath, List.of(), TestTaskSelector.REASON_REQUIRES_PACKAGING_HOST);
    }

    private static BaseTarget target(
        String project,
        String sourceSet,
        String fqcn,
        String compileTaskPath,
        List<String> runnableTasks,
        String skipReason
    ) {
        return new BaseTarget(
            project,
            sourceSet,
            sourceSet,
            fqcn,
            null,
            null,
            compileTaskPath,
            "/out/" + project,
            runnableTasks,
            runnableTasks.size(),
            skipReason
        );
    }
}
