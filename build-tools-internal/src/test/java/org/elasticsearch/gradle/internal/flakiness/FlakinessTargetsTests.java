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
 * Unit tests for the pure helpers that replaced the old root {@code flakinessMergeTargets} task: folding the
 * per-project resolve outputs back into one ordered target list, and deriving the compile task paths. The
 * end-to-end flow is exercised by {@code FlakinessResolvePluginFuncTest}.
 */
public class FlakinessTargetsTests {

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
            FlakinessTargets.compileTaskPaths(targets),
            contains(":libs:x:compileInternalClusterTestJava", ":qa:rolling:compileJavaRestTestJava", ":server:compileTestJava")
        );
    }

    @Test
    public void testNoCompileTasksWhenNothingIsRunnable() {
        List<BaseTarget> targets = List.of(skipped(":qa:a", "test", "org.foo.ATests", ":qa:a:compileTestJava"));
        assertThat(FlakinessTargets.compileTaskPaths(targets), is(empty()));
    }

    /**
     * The fold restores the order of the refs file even though the per-project files arrive sorted by project
     * path - that ordering is the reason each per-project entry carries its ref index.
     */
    @Test
    public void testMergeRestoresRefOrderAcrossProjects() {
        BaseTarget zeroth = runnable(":z:last", "test", "org.foo.ZTests", ":z:last:compileTestJava");
        BaseTarget first = runnable(":a:first", "test", "org.foo.ATests", ":a:first:compileTestJava");

        // File order is :a:first then :z:last, but the refs are the other way round.
        FlakinessJson.BaseTargetsFile merged = FlakinessTargets.merge(
            List.of(changedFile("z/src/test/java/org/foo/ZTests.java"), changedFile("a/src/test/java/org/foo/ATests.java")),
            List.of(
                new FlakinessJson.ProjectTargetsFile(":a:first", List.of(new FlakinessJson.RefTarget(1, first))),
                new FlakinessJson.ProjectTargetsFile(":z:last", List.of(new FlakinessJson.RefTarget(0, zeroth)))
            )
        );

        assertThat(merged.targets(), contains(zeroth, first));
        assertThat(merged.unresolved(), is(empty()));
    }

    /**
     * A class ref is only unresolved when <em>no</em> project claimed it - the one verdict a single project
     * cannot reach on its own. An unmatched changed-file ref stays silent (it is simply not a test).
     */
    @Test
    public void testUnresolvedOnlyForClassRefsNoProjectClaimed() {
        FlakinessRef claimedUnmute = new FlakinessRef(FlakinessRef.SOURCE_UNMUTE, null, "org.foo.ATests", null, null);
        FlakinessRef orphanUnmute = new FlakinessRef(FlakinessRef.SOURCE_UNMUTE, null, "org.foo.GoneTests", null, null);
        FlakinessRef orphanExplicit = new FlakinessRef(FlakinessRef.SOURCE_EXPLICIT, null, null, null, "org.foo.AlsoGoneTests");
        FlakinessRef orphanChangedFile = changedFile("docs/README.asciidoc");

        FlakinessJson.BaseTargetsFile merged = FlakinessTargets.merge(
            List.of(claimedUnmute, orphanUnmute, orphanExplicit, orphanChangedFile),
            List.of(
                new FlakinessJson.ProjectTargetsFile(
                    ":a",
                    List.of(new FlakinessJson.RefTarget(0, runnable(":a", "test", "org.foo.ATests", ":a:compileTestJava")))
                ),
                // A project that owns nothing still writes its (empty) share.
                new FlakinessJson.ProjectTargetsFile(":b", List.of())
            )
        );

        assertThat(merged.targets().size(), is(1));
        assertThat(merged.unresolved().stream().map(u -> u.ref()).toList(), contains(orphanUnmute, orphanExplicit));
        assertThat(merged.unresolved().get(0).reason(), is("no-source-file"));
    }

    /** Two projects resolving the same identity collapse to one target (the resolver's dedupe rule). */
    @Test
    public void testMergeDedupesIdenticalTargets() {
        BaseTarget target = runnable(":a", "test", "org.foo.ATests", ":a:compileTestJava");
        FlakinessJson.BaseTargetsFile merged = FlakinessTargets.merge(
            List.of(changedFile("a/src/test/java/org/foo/ATests.java")),
            List.of(
                new FlakinessJson.ProjectTargetsFile(":a", List.of(new FlakinessJson.RefTarget(0, target))),
                new FlakinessJson.ProjectTargetsFile(":a-copy", List.of(new FlakinessJson.RefTarget(0, target)))
            )
        );
        assertThat(merged.targets(), contains(target));
    }

    private static FlakinessRef changedFile(String path) {
        return new FlakinessRef(FlakinessRef.SOURCE_CHANGED_FILE, path, null, null, null);
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
