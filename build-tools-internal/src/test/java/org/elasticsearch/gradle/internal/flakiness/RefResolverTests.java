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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for the single-project resolver: does <em>this</em> project own a given ref, and if so what kind
 * of target is it and which of the project's {@code Test} tasks can re-run it? A fixture repo tree is created
 * on disk so the class-ref filesystem probe is exercised for real rather than stubbed.
 *
 * <p>Self-selection is the load-bearing decision of the design: a project that wrongly resolves nothing is a
 * false negative that reads as "no tests to re-run", and one that wrongly claims a foreign file attributes a
 * test to a project whose {@code Test} tasks cannot run it.
 *
 * <p>Tests that are only about membership or kind use {@link #membershipResolver}, which passes an empty
 * {@code Test}-task list. That isolates the two questions: the resolver only consults {@code Test} tasks
 * after it has already decided a ref belongs to one of the project's source sets.
 */
public class RefResolverTests {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    // ---- self-selection: does this project own the ref at all? ----

    @Test
    public void testClaimsChangedFileUnderItsOwnSourceSet() {
        Path repo = tmp.getRoot().toPath();
        RefResolver server = membershipResolver(repo, project(repo, ":server", "server", "test"));

        assertThat(server.resolve(changedFile("server/src/test/java/org/foo/BarTests.java")).isPresent(), is(true));
    }

    /** Outside the project directory entirely - rejected before any source set is consulted. */
    @Test
    public void testDoesNotClaimAFileOutsideItsProjectDirectory() {
        Path repo = tmp.getRoot().toPath();
        RefResolver server = membershipResolver(repo, project(repo, ":server", "server", "test"));

        assertThat(server.resolve(changedFile("libs/x/src/test/java/org/foo/BarTests.java")).isPresent(), is(false));
    }

    /**
     * Inside the project but under no test source set: a production file is simply not a test, so it is
     * ignored rather than reported unresolved.
     */
    @Test
    public void testDoesNotClaimAFileInItsProjectButOutsideEveryTestSourceSet() {
        Path repo = tmp.getRoot().toPath();
        RefResolver server = membershipResolver(repo, project(repo, ":server", "server", "test"));

        assertThat(server.resolve(changedFile("server/src/main/java/org/foo/NotATest.java")).isPresent(), is(false));
    }

    /**
     * The case a directory-prefix or nearest-ancestor heuristic cannot get right: the two projects'
     * directories are nested, but their {@code srcDirs} are disjoint, so exactly one of them claims the file.
     */
    @Test
    public void testNestedProjectsDisambiguateBySrcDirsNotByDirectoryNesting() {
        Path repo = tmp.getRoot().toPath();
        String logsdb = "x-pack/plugin/logsdb";
        String rollingUpgrade = logsdb + "/qa/rolling-upgrade";
        RefResolver outer = membershipResolver(repo, project(repo, ":x-pack:plugin:logsdb", logsdb, "javaRestTest"));
        RefResolver nested = membershipResolver(
            repo,
            project(repo, ":x-pack:plugin:logsdb:qa:rolling-upgrade", rollingUpgrade, "javaRestTest")
        );
        FlakinessRef ref = changedFile(rollingUpgrade + "/src/javaRestTest/java/org/foo/RollingUpgradeIT.java");

        assertThat("nested project owns the file", nested.resolve(ref).isPresent(), is(true));
        assertThat("ancestor project must not claim it", outer.resolve(ref).isPresent(), is(false));
    }

    /** A project with no test source sets (e.g. {@code :buildSrc}) can never claim a ref. */
    @Test
    public void testProjectWithNoSourceSetsClaimsNothing() {
        Path repo = tmp.getRoot().toPath();
        ProjectInfo buildSrc = new ProjectInfo(":buildSrc", repo.resolve("buildSrc"), List.of());
        RefResolver resolver = membershipResolver(repo, buildSrc);

        assertThat(resolver.resolve(unmute("org.foo.BarTests", null)).isPresent(), is(false));
    }

    // ---- changed-file refs: the claiming source set decides the kind ----

    /**
     * The extension decides the dispatch, but the claiming source set decides the kind - and the fqcn comes
     * from the path relative to that source set's java dir, not from any {@code src/<ss>/java} assumption.
     */
    @Test
    public void testChangedJavaFileTakesItsKindFromTheClaimingSourceSet() {
        Path repo = tmp.getRoot().toPath();
        ProjectInfo p = project(repo, ":server", "server", "test", "internalClusterTest", "javaRestTest");
        RefResolver server = membershipResolver(repo, p);

        BaseTarget unit = server.resolve(changedFile("server/src/test/java/org/elasticsearch/FooTests.java")).orElseThrow();
        assertThat(unit.gradleProject(), equalTo(":server"));
        assertThat(unit.sourceSet(), equalTo("test"));
        assertThat(unit.kind(), equalTo("test"));
        assertThat(unit.fqcn(), equalTo("org.elasticsearch.FooTests"));

        assertThat(kindOf(server, "server/src/internalClusterTest/java/org/elasticsearch/BarIT.java"), equalTo("internalClusterTest"));
        assertThat(kindOf(server, "server/src/javaRestTest/java/org/elasticsearch/BazIT.java"), equalTo("javaRestTest"));
    }

    /** A yaml suite is addressed by its suite path, not by a class, so it carries no fqcn. */
    @Test
    public void testChangedYamlSuiteResourceResolvesToItsSuitePathWithNoFqcn() {
        Path repo = tmp.getRoot().toPath();
        RefResolver esql = membershipResolver(repo, project(repo, ":x-pack:plugin:esql", "x-pack/plugin/esql", "yamlRestTest"));
        String yml = "x-pack/plugin/esql/src/yamlRestTest/resources/rest-api-spec/test/esql/10_foo.yml";

        BaseTarget suite = esql.resolve(changedFile(yml)).orElseThrow();

        assertThat(suite.gradleProject(), equalTo(":x-pack:plugin:esql"));
        assertThat(suite.kind(), equalTo("yamlRestTestSuite"));
        assertThat(suite.suitePath(), equalTo("esql/10_foo"));
        assertThat(suite.fqcn(), is(nullValue()));
    }

    // ---- class refs: resolved by probing for the source file on disk ----

    @Test
    public void testClaimsClassRefOnlyWhenTheSourceFileExistsOnDisk() throws IOException {
        Path repo = tmp.getRoot().toPath();
        writeJava(repo, "server/src/test/java/org/foo/PresentTests.java");
        RefResolver server = membershipResolver(repo, project(repo, ":server", "server", "test"));

        assertThat(server.resolve(unmute("org.foo.PresentTests", null)).isPresent(), is(true));
        assertThat(server.resolve(unmute("org.foo.AbsentTests", null)).isPresent(), is(false));
    }

    @Test
    public void testClassRefTakesItsKindFromTheSourceSetHoldingItsSourceFile() throws IOException {
        Path repo = tmp.getRoot().toPath();
        writeJava(repo, "server/src/internalClusterTest/java/org/elasticsearch/BarIT.java");
        RefResolver server = membershipResolver(repo, project(repo, ":server", "server", "test", "internalClusterTest"));

        BaseTarget integ = server.resolve(unmute("org.elasticsearch.BarIT", null)).orElseThrow();

        assertThat(integ.kind(), equalTo("internalClusterTest"));
        assertThat(integ.sourceSet(), equalTo("internalClusterTest"));
        assertThat(integ.fqcn(), equalTo("org.elasticsearch.BarIT"));
    }

    /**
     * A yaml-shaped method on a {@code yamlRestTest} runner narrows to that one parameterised case, so the
     * descriptor has to survive verbatim - it is what the generated {@code --tests} filter will match on.
     */
    @Test
    public void testYamlDescriptorOnAYamlRestTestClassResolvesToAParameterisedCase() throws IOException {
        Path repo = tmp.getRoot().toPath();
        writeJava(repo, "x-pack/plugin/esql/src/yamlRestTest/java/org/elasticsearch/EsqlIT.java");
        RefResolver esql = membershipResolver(repo, project(repo, ":x-pack:plugin:esql", "x-pack/plugin/esql", "yamlRestTest"));

        BaseTarget yamlCase = esql.resolve(unmute("org.elasticsearch.EsqlIT", "test {yaml=esql/10_foo/Case}")).orElseThrow();

        assertThat(yamlCase.kind(), equalTo("yamlRestTestCase"));
        assertThat(yamlCase.fqcn(), equalTo("org.elasticsearch.EsqlIT"));
        assertThat(yamlCase.yamlTest(), equalTo("test {yaml=esql/10_foo/Case}"));
    }

    /**
     * An {@code unmute} and an {@code explicit} ref naming the same class must produce an <em>equal</em>
     * target - the resolver-side precondition that lets the global fold collapse them to one command. The
     * collapse itself is {@code FlakinessTargetsTests#testMergeDedupesIdenticalTargets}.
     */
    @Test
    public void testExplicitSpecAndUnmuteOfTheSameClassResolveToTheIdenticalTarget() throws IOException {
        Path repo = tmp.getRoot().toPath();
        writeJava(repo, "server/src/test/java/org/elasticsearch/FooTests.java");
        RefResolver server = membershipResolver(repo, project(repo, ":server", "server", "test"));

        BaseTarget viaUnmute = server.resolve(unmute("org.elasticsearch.FooTests", null)).orElseThrow();
        BaseTarget viaExplicit = server.resolve(explicit("org.elasticsearch.FooTests.testX")).orElseThrow();

        assertThat(viaExplicit, equalTo(viaUnmute));
    }

    // ---- disposition: which Test tasks can actually re-run the target ----

    /** An ordinary project resolves to its plain, enabled bare task - derived from the model, not assumed. */
    @Test
    public void testTargetCarriesTheEnabledBareTaskForAnOrdinaryProject() {
        Path repo = tmp.getRoot().toPath();
        ProjectInfo p = project(repo, ":server", "server", "test");
        RefResolver server = resolver(repo, p, enabledBareTasks(p));

        BaseTarget unit = server.resolve(changedFile("server/src/test/java/org/elasticsearch/FooTests.java")).orElseThrow();

        assertThat(unit.runnable(), is(true));
        assertThat(unit.runnableTasks(), contains(":server:test"));
        assertThat(unit.candidateTasks(), equalTo(1));
        assertThat(unit.skipReason(), is(nullValue()));
    }

    /**
     * The bwc project's bare {@code javaRestTest} task is disabled, so the target must resolve to the real
     * {@code bwcTest} tasks (capped, newest first) instead of the task Gradle would report SKIPPED.
     */
    @Test
    public void testTargetCarriesRealBwcTasksWhenTheBareTaskIsDisabled() {
        Path repo = tmp.getRoot().toPath();
        ProjectInfo p = project(repo, ":qa:rolling", "qa/rolling", "javaRestTest");
        RefResolver bwc = resolver(repo, p, bwcShapedTasks(p));

        BaseTarget target = bwc.resolve(changedFile("qa/rolling/src/javaRestTest/java/org/elasticsearch/SomeIT.java")).orElseThrow();

        assertThat(target.gradleProject(), equalTo(":qa:rolling"));
        assertThat(target.runnable(), is(true));
        assertThat(target.runnableTasks(), contains(":qa:rolling:v9.6.0#bwcTest", ":qa:rolling:v9.5.1#bwcTest"));
        assertThat(target.candidateTasks(), equalTo(3));
    }

    // ---- malformed input ----

    /**
     * A malformed refs file is an input defect, not a programming error: a missing or misspelled
     * {@code source} must resolve to nothing rather than throw an NPE out of the switch. This runs in every
     * project at once, so an NPE here surfaces as a wall of unreadable stack traces with no mention of the
     * refs file.
     *
     * <p>Only the no-throw property is asserted here. Turning "nothing claimed it" into
     * {@code unknown-source} is {@link FlakinessTargets#merge}'s job, since only it has the global view, and
     * {@code FlakinessTargetsTests#testUnknownRefSourceIsReportedNotSilentlyDropped} pins that.
     */
    @Test
    public void testMalformedRefSourceResolvesToNothingRatherThanThrowing() {
        Path repo = tmp.getRoot().toPath();
        RefResolver server = membershipResolver(repo, project(repo, ":server", "server", "test"));

        FlakinessRef nullSource = new FlakinessRef(null, null, "org.elasticsearch.FooTests", null, null);
        FlakinessRef bogusSource = new FlakinessRef("typo-source", "server/src/test/java/X.java", null, null, null);

        assertThat(server.resolve(nullSource).isPresent(), is(false));
        assertThat(server.resolve(bogusSource).isPresent(), is(false));
    }

    // ---- parseSpec ----

    /** The last dot splits class from method, and a spec with no method tail keeps a null method. */
    @Test
    public void testParseSpecSplitsAClassDotMethodSpec() {
        assertThat(RefResolver.parseSpec("org.foo.BarTests").className(), equalTo("org.foo.BarTests"));
        assertThat(RefResolver.parseSpec("org.foo.BarTests").method(), is(nullValue()));
        assertThat(RefResolver.parseSpec("org.foo.BarTests.testX").className(), equalTo("org.foo.BarTests"));
        assertThat(RefResolver.parseSpec("org.foo.BarTests.testX").method(), equalTo("testX"));
    }

    /** The yaml branch exists because the plain last-dot rule would mangle {@code test {yaml=a/b}}. */
    @Test
    public void testParseSpecKeepsAWholeYamlDescriptorAsTheMethod() {
        assertThat(RefResolver.parseSpec("org.foo.YamlIT.test {yaml=a/b}").className(), equalTo("org.foo.YamlIT"));
        assertThat(RefResolver.parseSpec("org.foo.YamlIT.test {yaml=a/b}").method(), equalTo("test {yaml=a/b}"));
    }

    // ---- fixtures ----

    /** A project owning the named java source sets at the conventional ES layout under the fixture repo. */
    private static ProjectInfo project(Path repo, String projectPath, String relativeDir, String... sourceSets) {
        return new ProjectInfo(
            projectPath,
            repo.resolve(relativeDir),
            Arrays.stream(sourceSets).map(ss -> ssi(repo, relativeDir, ss)).toList()
        );
    }

    /**
     * Build an authoritative {@link SourceSetInfo} matching the conventional ES layout. The resolver works
     * off these real dirs, so the fixture - not a hard-coded convention - is what it reads.
     */
    private static SourceSetInfo ssi(Path repo, String projRel, String ssName) {
        Path base = repo.resolve(projRel).resolve("src").resolve(ssName);
        return new SourceSetInfo(
            ssName,
            List.of(base.resolve("java")),
            List.of(base.resolve("resources")),
            repo.resolve(projRel).resolve("build/classes/java/" + ssName)
        );
    }

    /**
     * A resolver with an empty {@code Test}-task lookup, for the membership and kind questions. Every target
     * then comes back with {@link TestTaskSelector#REASON_NO_RUNNABLE_TASK}, so these tests assert ownership
     * without fabricating {@code Test}-task facts they do not care about.
     */
    private static RefResolver membershipResolver(Path repo, ProjectInfo p) {
        return new RefResolver(repo, p, List.of(), 0);
    }

    private static RefResolver resolver(Path repo, ProjectInfo p, List<TestTaskInfo> tasks) {
        return new RefResolver(repo, p, tasks, TestTaskSelector.DEFAULT_TASK_CAP);
    }

    /** What the build reports for an ordinary project: one enabled bare task per source set. */
    private static List<TestTaskInfo> enabledBareTasks(ProjectInfo p) {
        return p.sourceSets().stream().map(ss -> testTask(p.projectPath(), ss.name(), true, ss.outputDir())).toList();
    }

    /**
     * The {@code elasticsearch.bwc-test} shape: a <em>disabled</em> bare task plus differently named tasks
     * pointed at the same source-set output, which is why the disposition cannot just assume the bare task.
     */
    private static List<TestTaskInfo> bwcShapedTasks(ProjectInfo p) {
        List<TestTaskInfo> tasks = new ArrayList<>();
        for (SourceSetInfo ss : p.sourceSets()) {
            tasks.add(testTask(p.projectPath(), ss.name(), false, ss.outputDir()));
            tasks.add(testTask(p.projectPath(), "bcUpgradeTest", true, ss.outputDir()));
            tasks.add(testTask(p.projectPath(), "v9.5.1#bwcTest", true, ss.outputDir()));
            tasks.add(testTask(p.projectPath(), "v9.6.0#bwcTest", true, ss.outputDir()));
        }
        return tasks;
    }

    private static TestTaskInfo testTask(String projectPath, String name, boolean enabled, Path classesDir) {
        return new TestTaskInfo(name, projectPath + ":" + name, enabled, List.of(classesDir));
    }

    private static String kindOf(RefResolver resolver, String changedPath) {
        return resolver.resolve(changedFile(changedPath)).orElseThrow().kind();
    }

    /** The resolver only calls {@link Files#isRegularFile}, so the content is irrelevant. */
    private static void writeJava(Path repo, String relativePath) throws IOException {
        Path file = repo.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, "// fixture\n");
    }

    private static FlakinessRef changedFile(String path) {
        return new FlakinessRef(FlakinessRef.SOURCE_CHANGED_FILE, path, null, null, null);
    }

    private static FlakinessRef unmute(String className, String method) {
        return new FlakinessRef(FlakinessRef.SOURCE_UNMUTE, null, className, method, null);
    }

    private static FlakinessRef explicit(String spec) {
        return new FlakinessRef(FlakinessRef.SOURCE_EXPLICIT, null, null, null, spec);
    }
}
