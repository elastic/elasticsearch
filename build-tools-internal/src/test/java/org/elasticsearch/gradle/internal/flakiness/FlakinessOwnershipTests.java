/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for the two pure decisions the per-project topology rests on: whether a project claims a ref
 * ({@link FlakinessProjectResolvePlugin#ownsAnyRef}), and the project-path to file-name mapping every project
 * writes its share of the answer under ({@link FlakinessProjectResolvePlugin#fileBaseName}).
 *
 * <p>Ownership is the load-bearing decision of the design: a project that wrongly answers {@code false}
 * silently resolves nothing (a false negative that reads as "no tests to re-run"), and one that wrongly
 * answers {@code true} pays the {@code Test}-task realization cost the cheap exit exists to avoid.
 */
public class FlakinessOwnershipTests {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testClaimsChangedFileUnderItsOwnSourceSet() throws IOException {
        Path repo = tmp.getRoot().toPath();
        ProjectInfo server = project(repo, ":server", "server", "test");
        writeJava(repo, "server/src/test/java/org/foo/BarTests.java");

        String refs = changedFileRefs("server/src/test/java/org/foo/BarTests.java");
        assertThat(FlakinessProjectResolvePlugin.ownsAnyRef(repo, server, refs), is(true));
    }

    @Test
    public void testDoesNotClaimChangedFileOutsideItsSourceSets() throws IOException {
        Path repo = tmp.getRoot().toPath();
        ProjectInfo server = project(repo, ":server", "server", "test");
        writeJava(repo, "libs/x/src/test/java/org/foo/BarTests.java");

        String refs = changedFileRefs("libs/x/src/test/java/org/foo/BarTests.java");
        assertThat(FlakinessProjectResolvePlugin.ownsAnyRef(repo, server, refs), is(false));
    }

    /**
     * The case a directory-prefix or nearest-ancestor heuristic cannot get right: the two projects' directories
     * are nested, but their {@code srcDirs} are disjoint, so exactly one of them claims the file.
     */
    @Test
    public void testNestedProjectsDisambiguateBySrcDirsNotByDirectoryNesting() throws IOException {
        Path repo = tmp.getRoot().toPath();
        ProjectInfo outer = project(repo, ":x-pack:plugin:logsdb", "x-pack/plugin/logsdb", "javaRestTest");
        ProjectInfo nested = project(
            repo,
            ":x-pack:plugin:logsdb:qa:rolling-upgrade",
            "x-pack/plugin/logsdb/qa/rolling-upgrade",
            "javaRestTest"
        );
        String file = "x-pack/plugin/logsdb/qa/rolling-upgrade/src/javaRestTest/java/org/foo/RollingUpgradeIT.java";
        writeJava(repo, file);

        String refs = changedFileRefs(file);
        assertThat("nested project owns the file", FlakinessProjectResolvePlugin.ownsAnyRef(repo, nested, refs), is(true));
        assertThat("ancestor project must not claim it", FlakinessProjectResolvePlugin.ownsAnyRef(repo, outer, refs), is(false));
    }

    @Test
    public void testClaimsClassRefOnlyWhenTheSourceFileExistsOnDisk() throws IOException {
        Path repo = tmp.getRoot().toPath();
        ProjectInfo server = project(repo, ":server", "server", "test");
        writeJava(repo, "server/src/test/java/org/foo/PresentTests.java");

        assertThat(FlakinessProjectResolvePlugin.ownsAnyRef(repo, server, unmuteRefs("org.foo.PresentTests")), is(true));
        assertThat(FlakinessProjectResolvePlugin.ownsAnyRef(repo, server, unmuteRefs("org.foo.AbsentTests")), is(false));
    }

    @Test
    public void testNoSourceSetsOrNoRefsMeansNoOwnership() {
        Path repo = tmp.getRoot().toPath();
        ProjectInfo noSourceSets = new ProjectInfo(":buildSrc", repo.resolve("buildSrc"), List.of());
        ProjectInfo server = project(repo, ":server", "server", "test");

        assertThat(FlakinessProjectResolvePlugin.ownsAnyRef(repo, noSourceSets, unmuteRefs("org.foo.BarTests")), is(false));
        assertThat(FlakinessProjectResolvePlugin.ownsAnyRef(repo, server, "{\"refs\":[]}"), is(false));
        assertThat(FlakinessProjectResolvePlugin.ownsAnyRef(repo, server, null), is(false));
        assertThat(FlakinessProjectResolvePlugin.ownsAnyRef(repo, server, "  "), is(false));
    }

    // ---- fileBaseName ----

    @Test
    public void testFileBaseNameMapsProjectPathsToReadableNames() {
        assertThat(FlakinessProjectResolvePlugin.fileBaseName(":"), is("root"));
        assertThat(FlakinessProjectResolvePlugin.fileBaseName(":server"), is("server"));
        assertThat(FlakinessProjectResolvePlugin.fileBaseName(":x-pack:plugin:logsdb"), is("x-pack.plugin.logsdb"));
    }

    /**
     * Every project writes into one shared directory, so a collision would make one task silently overwrite
     * another's output. Segment names come from directory names and may therefore contain {@code .}.
     */
    @Test
    public void testFileBaseNameIsInjectiveForDottedSegments() {
        String dottedSegment = FlakinessProjectResolvePlugin.fileBaseName(":libs:x.y");
        String nestedProject = FlakinessProjectResolvePlugin.fileBaseName(":libs:x:y");
        assertThat(dottedSegment, is(not(nestedProject)));
    }

    // ---- helpers ----

    /** A project owning a single java source set at the conventional {@code src/<sourceSet>/java} location. */
    private static ProjectInfo project(Path repo, String projectPath, String relativeDir, String sourceSet) {
        Path projectDir = repo.resolve(relativeDir);
        SourceSetInfo ss = new SourceSetInfo(
            sourceSet,
            List.of(projectDir.resolve("src/" + sourceSet + "/java")),
            List.of(projectDir.resolve("src/" + sourceSet + "/resources")),
            projectDir.resolve("build/classes/java/" + sourceSet),
            ":" + relativeDir.replace('/', ':') + ":compile" + capitalize(sourceSet) + "Java"
        );
        return new ProjectInfo(projectPath, projectDir, List.of(ss));
    }

    private static String capitalize(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static void writeJava(Path repo, String relativePath) throws IOException {
        Path file = repo.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, "class Fixture {}");
    }

    private static String changedFileRefs(String path) {
        return "{\"refs\":[{\"source\":\"changed-file\",\"path\":\"" + path + "\"}]}";
    }

    private static String unmuteRefs(String className) {
        return "{\"refs\":[{\"source\":\"unmute\",\"className\":\"" + className + "\"}]}";
    }
}
