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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for the pure helpers that replaced the old root {@code flakinessMergeTargets} task: folding the
 * per-project resolve outputs back into one ordered target list, and unioning their class directories. The
 * end-to-end flow is exercised by {@code FlakinessResolvePluginFuncTest}.
 */
public class FlakinessTargetsTests {

    /**
     * The scan set is the union across <em>every</em> project's file, including projects that resolved nothing.
     * That is the whole point of compiling everything: an abstract base in one project is only connected to
     * concrete subclasses in another when both projects' output is in the set.
     */
    @Test
    public void testClassDirsUnionSpansProjectsThatOwnNoRef() {
        List<FlakinessJson.ProjectTargetsFile> perProject = List.of(
            new FlakinessJson.ProjectTargetsFile(
                ":server",
                List.of(new FlakinessJson.RefTarget(0, runnable(":server", "test", "org.foo.ATests"))),
                List.of(Path.of("/out/server/main"), Path.of("/out/server/test"))
            , List.of()),
            // Owns nothing, but still contributes its bytecode - this is the cross-project case.
            new FlakinessJson.ProjectTargetsFile(
                ":test:framework",
                List.of(),
                List.of(Path.of("/out/framework/main"))
            , List.of()),
            // Overlapping entry -> collapsed.
            new FlakinessJson.ProjectTargetsFile(":dup", List.of(), List.of(Path.of("/out/server/main")), List.of())
        );

        assertThat(
            FlakinessTargets.classDirs(perProject),
            contains(Path.of("/out/framework/main"), Path.of("/out/server/main"), Path.of("/out/server/test"))
        );
    }

    /**
     * The dispositions index is what lets the scan run a subclass it found in a project no ref pointed at: the
     * lookup key is the compiled-output directory, so it works across projects and across source sets of the
     * same project.
     */
    @Test
    public void testDispositionsAreIndexedByOutputDirAcrossProjects() {
        SourceSetDisposition serverTest = disposition("test", "/out/server/test", List.of(":server:test"));
        SourceSetDisposition serverIct = disposition("internalClusterTest", "/out/server/ict", List.of(":server:internalClusterTest"));
        SourceSetDisposition mlTest = disposition("test", "/out/ml/test", List.of(":ml:test"));

        Map<Path, FlakinessTargets.OwnedSourceSet> byDir = FlakinessTargets.dispositionsByClassDir(
            List.of(
                new FlakinessJson.ProjectTargetsFile(":server", List.of(), List.of(), List.of(serverTest, serverIct)),
                // Resolved nothing, yet its disposition must still be reachable - that is the point.
                new FlakinessJson.ProjectTargetsFile(":ml", List.of(), List.of(), List.of(mlTest))
            )
        );

        assertThat(byDir.get(Path.of("/out/ml/test")).projectPath(), is(":ml"));
        assertThat(byDir.get(Path.of("/out/ml/test")).disposition().runnableTasks(), contains(":ml:test"));
        // Two source sets of ONE project stay distinct: they are different Test tasks.
        assertThat(byDir.get(Path.of("/out/server/test")).disposition().sourceSet(), is("test"));
        assertThat(byDir.get(Path.of("/out/server/ict")).disposition().sourceSet(), is("internalClusterTest"));
        assertThat(byDir.get(Path.of("/out/nowhere")), is(nullValue()));
    }

    @Test
    public void testDispositionsToleratesMissingLists() {
        Map<Path, FlakinessTargets.OwnedSourceSet> byDir = FlakinessTargets.dispositionsByClassDir(
            List.of(
                new FlakinessJson.ProjectTargetsFile(":a", List.of(), List.of(), null),
                new FlakinessJson.ProjectTargetsFile(":b", List.of(), List.of(), List.of())
            )
        );
        assertThat(byDir.isEmpty(), is(true));
    }

    private static SourceSetDisposition disposition(String sourceSet, String outputDir, List<String> tasks) {
        return new SourceSetDisposition(sourceSet, Path.of(outputDir), sourceSet, tasks, tasks.size(), null);
    }

    /** A file written by an older resolve (or a hand-crafted one) may carry no classDirs at all. */
    @Test
    public void testClassDirsToleratesMissingAndEmptyLists() {
        List<FlakinessJson.ProjectTargetsFile> perProject = List.of(
            new FlakinessJson.ProjectTargetsFile(":a", List.of(), null, List.of()),
            new FlakinessJson.ProjectTargetsFile(":b", List.of(), List.of(), List.of())
        );
        assertThat(FlakinessTargets.classDirs(perProject), is(empty()));
    }

    /**
     * The fold restores the order of the refs file even though the per-project files arrive sorted by project
     * path - that ordering is the reason each per-project entry carries its ref index.
     */
    @Test
    public void testMergeRestoresRefOrderAcrossProjects() {
        BaseTarget zeroth = runnable(":z:last", "test", "org.foo.ZTests");
        BaseTarget first = runnable(":a:first", "test", "org.foo.ATests");

        // File order is :a:first then :z:last, but the refs are the other way round.
        FlakinessJson.BaseTargetsFile merged = FlakinessTargets.merge(
            List.of(changedFile("z/src/test/java/org/foo/ZTests.java"), changedFile("a/src/test/java/org/foo/ATests.java")),
            List.of(
                new FlakinessJson.ProjectTargetsFile(":a:first", List.of(new FlakinessJson.RefTarget(1, first)), List.of(), List.of()),
                new FlakinessJson.ProjectTargetsFile(":z:last", List.of(new FlakinessJson.RefTarget(0, zeroth)), List.of(), List.of())
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
                    List.of(new FlakinessJson.RefTarget(0, runnable(":a", "test", "org.foo.ATests"))),
                    List.of()
                , List.of()),
                // A project that owns nothing still writes its (empty) share.
                new FlakinessJson.ProjectTargetsFile(":b", List.of(), List.of(), List.of())
            )
        );

        assertThat(merged.targets().size(), is(1));
        assertThat(merged.unresolved().stream().map(u -> u.ref()).toList(), contains(orphanUnmute, orphanExplicit));
        assertThat(merged.unresolved().get(0).reason(), is(RefResolver.REASON_NO_SOURCE_FILE));
    }

    /**
     * A ref whose {@code source} this resolver does not know is a TS/Java contract defect, so it must be
     * reported. The per-project task discards {@link RefResolver}'s own unresolved verdicts (they mean "not in
     * THIS project", not "not anywhere"), so the merge is the only place left that can surface it - without
     * this the ref would vanish and the drift would read as "nothing to run".
     */
    @Test
    public void testUnknownRefSourceIsReportedNotSilentlyDropped() {
        FlakinessRef futureSource = new FlakinessRef("some-future-source", null, "org.foo.ATests", null, null);
        FlakinessRef missingSource = new FlakinessRef(null, null, "org.foo.BTests", null, null);

        FlakinessJson.BaseTargetsFile merged = FlakinessTargets.merge(
            List.of(futureSource, missingSource),
            List.of(new FlakinessJson.ProjectTargetsFile(":a", List.of(), List.of(), List.of()))
        );

        assertThat(merged.targets(), is(empty()));
        assertThat(merged.unresolved().stream().map(u -> u.ref()).toList(), contains(futureSource, missingSource));
        assertThat(
            merged.unresolved().stream().map(u -> u.reason()).distinct().toList(),
            contains(RefResolver.REASON_UNKNOWN_SOURCE)
        );
    }

    /** Two projects resolving the same identity collapse to one target (the resolver's dedupe rule). */
    @Test
    public void testMergeDedupesIdenticalTargets() {
        BaseTarget target = runnable(":a", "test", "org.foo.ATests");
        FlakinessJson.BaseTargetsFile merged = FlakinessTargets.merge(
            List.of(changedFile("a/src/test/java/org/foo/ATests.java")),
            List.of(
                new FlakinessJson.ProjectTargetsFile(":a", List.of(new FlakinessJson.RefTarget(0, target)), List.of(), List.of()),
                new FlakinessJson.ProjectTargetsFile(":a-copy", List.of(new FlakinessJson.RefTarget(0, target)), List.of(), List.of())
            )
        );
        assertThat(merged.targets(), contains(target));
    }

    private static FlakinessRef changedFile(String path) {
        return new FlakinessRef(FlakinessRef.SOURCE_CHANGED_FILE, path, null, null, null);
    }

    private static BaseTarget runnable(String project, String sourceSet, String fqcn) {
        return target(project, sourceSet, fqcn, List.of(project + ":" + sourceSet), null);
    }

    private static BaseTarget target(String project, String sourceSet, String fqcn, List<String> runnableTasks, String skipReason) {
        return new BaseTarget(project, sourceSet, sourceSet, fqcn, null, null, runnableTasks, runnableTasks.size(), skipReason);
    }
}
