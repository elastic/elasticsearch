/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.elasticsearch.gradle.internal.flakiness.FlakinessPlan.PlanEntry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for the pure resolution + enrichment core (no Gradle). Fixture bytecode is generated in-test
 * with ASM so the {@link ClassHierarchyScanner} runs against real {@code .class} files, and a fixture repo
 * tree is created on disk so {@link RefResolver}'s class-ref filesystem probe is exercised end-to-end.
 */
public class FlakinessResolverTests {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    // ---- ClassHierarchyScanner (ASM) ----

    @Test
    public void testExpandsAbstractBaseToSortedCappedConcreteDescendants() throws IOException {
        Path classes = tmp.newFolder("classes").toPath();
        // AbstractFooTests <- {BarTests, BazTests, MidTests(abstract) <- LeafTests}, plus an unrelated class.
        writeClass(classes, "com/example/AbstractFooTests", "java/lang/Object", true);
        writeClass(classes, "com/example/BarTests", "com/example/AbstractFooTests", false);
        writeClass(classes, "com/example/BazTests", "com/example/AbstractFooTests", false);
        writeClass(classes, "com/example/MidTests", "com/example/AbstractFooTests", true);
        writeClass(classes, "com/example/LeafTests", "com/example/MidTests", false);
        writeClass(classes, "com/example/StandaloneTests", "java/lang/Object", false);

        ClassHierarchyScanner scanner = ClassHierarchyScanner.scan(List.of(classes));

        assertThat(scanner.isAbstract("com.example.AbstractFooTests"), is(true));
        assertThat(scanner.isAbstract("com.example.BarTests"), is(false));

        ClassHierarchyScanner.Expansion all = scanner.expand("com.example.AbstractFooTests", 5);
        assertThat(all.wasAbstract(), is(true));
        assertThat(all.totalConcrete(), equalTo(3));
        // Deterministic sorted FQCN order; MidTests excluded (abstract), LeafTests included (transitive).
        assertThat(all.toRun(), contains("com.example.BarTests", "com.example.BazTests", "com.example.LeafTests"));

        ClassHierarchyScanner.Expansion capped = scanner.expand("com.example.AbstractFooTests", 2);
        assertThat(capped.totalConcrete(), equalTo(3));
        assertThat(capped.toRun(), contains("com.example.BarTests", "com.example.BazTests"));
    }

    @Test
    public void testConcreteAndUnknownClassesPassThrough() throws IOException {
        Path classes = tmp.newFolder("classes").toPath();
        writeClass(classes, "com/example/BarTests", "java/lang/Object", false);
        ClassHierarchyScanner scanner = ClassHierarchyScanner.scan(List.of(classes));

        ClassHierarchyScanner.Expansion concrete = scanner.expand("com.example.BarTests", 5);
        assertThat(concrete.wasAbstract(), is(false));
        assertThat(concrete.toRun(), contains("com.example.BarTests"));

        ClassHierarchyScanner.Expansion unknown = scanner.expand("com.example.NotCompiled", 5);
        assertThat(unknown.wasAbstract(), is(false));
        assertThat(unknown.toRun(), contains("com.example.NotCompiled"));
    }

    // ---- RefResolver ----

    @Test
    public void testResolvesChangedFilesToProjectSourceSetKind() throws IOException {
        Path repo = tmp.newFolder("repo").toPath();
        List<ProjectInfo> projects = fixtureProjects(repo);

        FlakinessJson.BaseTargetsFile r = resolveAcross(
            repo,
            projects,
            List.of(
                changedFile("server/src/test/java/org/elasticsearch/FooTests.java"),
                changedFile("server/src/internalClusterTest/java/org/elasticsearch/BarIT.java"),
                changedFile("x-pack/plugin/esql/src/yamlRestTest/resources/rest-api-spec/test/esql/10_foo.yml"),
                changedFile("qa/rolling/src/javaRestTest/java/org/elasticsearch/SomeIT.java"),
                changedFile("server/src/main/java/org/elasticsearch/NotATest.java") // ignored, not a test file
            )
        );

        assertThat(r.unresolved(), is(empty()));
        assertThat(r.targets(), hasSize(4));

        BaseTarget unit = findByFqcn(r.targets(), "org.elasticsearch.FooTests");
        assertThat(unit.gradleProject(), equalTo(":server"));
        assertThat(unit.sourceSet(), equalTo("test"));
        assertThat(unit.kind(), equalTo("test"));
        // An ordinary project resolves to its plain, enabled bare task - now derived, not assumed.
        assertThat(unit.runnableTasks(), contains(":server:test"));
        assertThat(unit.skipReason(), is(nullValue()));

        BaseTarget integ = findByFqcn(r.targets(), "org.elasticsearch.BarIT");
        assertThat(integ.kind(), equalTo("internalClusterTest"));

        BaseTarget suite = r.targets().stream().filter(t -> "yamlRestTestSuite".equals(t.kind())).findFirst().orElseThrow();
        assertThat(suite.gradleProject(), equalTo(":x-pack:plugin:esql"));
        assertThat(suite.suitePath(), equalTo("esql/10_foo"));
        assertThat(suite.fqcn(), is(nullValue()));

        // The bwc project's bare javaRestTest task is disabled; the target resolves to its real bwcTest tasks
        // (capped, newest first) instead of the task Gradle would report SKIPPED.
        BaseTarget bwc = r.targets().stream().filter(t -> "javaRestTest".equals(t.kind())).findFirst().orElseThrow();
        assertThat(bwc.gradleProject(), equalTo(":qa:rolling"));
        assertThat(bwc.runnable(), is(true));
        assertThat(bwc.runnableTasks(), contains(":qa:rolling:v9.6.0#bwcTest", ":qa:rolling:v9.5.1#bwcTest"));
        assertThat(bwc.candidateTasks(), equalTo(3));
    }

    @Test
    public void testResolvesClassAndExplicitRefsViaFilesystemProbe() throws IOException {
        Path repo = tmp.newFolder("repo").toPath();
        List<ProjectInfo> projects = fixtureProjects(repo);
        touch(repo, "server/src/test/java/org/elasticsearch/FooTests.java");
        touch(repo, "x-pack/plugin/esql/src/yamlRestTest/java/org/elasticsearch/EsqlIT.java");

        FlakinessJson.BaseTargetsFile r = resolveAcross(
            repo,
            projects,
            List.of(
                unmute("org.elasticsearch.FooTests", null),
                unmute("org.elasticsearch.EsqlIT", "test {yaml=esql/10_foo/Case}"),
                explicit("org.elasticsearch.FooTests.testX"),
                unmute("org.elasticsearch.DoesNotExist", null)
            )
        );

        // FooTests appears once (unmute + explicit dedupe to the same target).
        assertThat(r.targets(), hasSize(2));
        BaseTarget foo = findByFqcn(r.targets(), "org.elasticsearch.FooTests");
        assertThat(foo.kind(), equalTo("test"));

        BaseTarget yamlCase = findByFqcn(r.targets(), "org.elasticsearch.EsqlIT");
        assertThat(yamlCase.kind(), equalTo("yamlRestTestCase"));
        assertThat(yamlCase.yamlTest(), equalTo("test {yaml=esql/10_foo/Case}"));

        assertThat(r.unresolved(), hasSize(1));
        assertThat(r.unresolved().get(0).reason(), equalTo(FlakinessPlan.REASON_NO_SOURCE_FILE));
        assertThat(r.unresolved().get(0).ref().className(), equalTo("org.elasticsearch.DoesNotExist"));
    }

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
    public void testMalformedRefSourceResolvesToNothingRatherThanThrowing() throws IOException {
        Path repo = tmp.newFolder("malformed-repo").toPath();
        ProjectInfo server = new ProjectInfo(":server", repo.resolve("server"), List.of(ssi(repo, "server", "test")));
        RefResolver resolver = resolver(repo, server);

        FlakinessRef nullSource = new FlakinessRef(null, null, "org.elasticsearch.FooTests", null, null);
        FlakinessRef bogusSource = new FlakinessRef("typo-source", "server/src/test/java/X.java", null, null, null);

        assertThat(resolver.resolve(nullSource).isPresent(), is(false));
        assertThat(resolver.resolve(bogusSource).isPresent(), is(false));
    }

    // ---- PlanBuilder ----

    @Test
    public void testFlattensAbstractSkipsUnrunnableAndPassesYamlThrough() throws IOException {
        Path classes = tmp.newFolder("classes").toPath();
        writeClass(classes, "com/example/AbstractFooTests", "java/lang/Object", true);
        writeClass(classes, "com/example/BarTests", "com/example/AbstractFooTests", false);
        writeClass(classes, "com/example/BazTests", "com/example/AbstractFooTests", false);
        writeClass(classes, "com/example/LoneAbstractTests", "java/lang/Object", true);
        ClassHierarchyScanner scanner = ClassHierarchyScanner.scan(List.of(classes));

        List<BaseTarget> targets = List.of(
            planTarget(":a", "test", "test", "com.example.AbstractFooTests", null, List.of(":a:test"), 1, null),
            // Nothing can run this one (the packaging policy) -> a skip entry with that reason.
            planTarget(":b", "test", "test", "org.foo.SomeTests", null, List.of(), 12, TestTaskSelector.REASON_REQUIRES_PACKAGING_HOST),
            planTarget(":c", "yamlRestTest", "yamlRestTestSuite", null, "esql/10_foo", List.of(":c:yamlRestTest"), 1, null),
            planTarget(":d", "test", "test", "com.example.LoneAbstractTests", null, List.of(":d:test"), 1, null),
            // A capped fan-out -> reported in taskSelections.
            planTarget(":e", "javaRestTest", "javaRestTest", "org.foo.SomeIT", null, List.of(":e:v9.6.0#bwcTest"), 67, null)
        );

        FlakinessPlan plan = PlanBuilder.build(targets, List.of(), scanner, 5, 1);

        // Abstract flattened into 2 concrete run entries, each with expandedFrom.
        List<PlanEntry> expanded = plan.entries().stream().filter(e -> "com.example.AbstractFooTests".equals(e.expandedFrom())).toList();
        assertThat(expanded, hasSize(2));
        assertThat(
            expanded.stream().map(PlanEntry::fqcn).toList(),
            containsInAnyOrder("com.example.BarTests", "com.example.BazTests")
        );
        assertThat(expanded.get(0).disposition(), equalTo("run"));

        assertThat(plan.expansions(), hasSize(1));
        assertThat(plan.expansions().get(0).abstractFqcn(), equalTo("com.example.AbstractFooTests"));
        assertThat(plan.expansions().get(0).ran(), equalTo(2));
        assertThat(plan.expansions().get(0).total(), equalTo(2));

        // No runnable task -> skip carrying the precise reason.
        PlanEntry unrunnable = plan.entries().stream().filter(e -> ":b".equals(e.gradleProject())).findFirst().orElseThrow();
        assertThat(unrunnable.disposition(), equalTo("skip"));
        assertThat(unrunnable.reason(), equalTo("requires-packaging-host"));
        assertThat(unrunnable.runnableTasks(), is(empty()));

        // yaml -> run pass-through, carrying its real task path.
        PlanEntry yaml = plan.entries().stream().filter(e -> ":c".equals(e.gradleProject())).findFirst().orElseThrow();
        assertThat(yaml.disposition(), equalTo("run"));
        assertThat(yaml.suitePath(), equalTo("esql/10_foo"));
        assertThat(yaml.runnableTasks(), contains(":c:yamlRestTest"));

        // Only the capped fan-out is reported; the 1-of-1 selections are not noise in the report.
        assertThat(plan.taskSelections(), hasSize(1));
        assertThat(plan.taskSelections().get(0).gradleProject(), equalTo(":e"));
        assertThat(plan.taskSelections().get(0).total(), equalTo(67));
        assertThat(plan.taskSelections().get(0).cap(), equalTo(1));
        assertThat(plan.taskSelections().get(0).selected(), contains(":e:v9.6.0#bwcTest"));

        // Abstract with no concrete subclass -> surfaced as unresolved, never silently dropped.
        assertThat(plan.unresolved(), hasSize(1));
        assertThat(plan.unresolved().get(0).reason(), equalTo(FlakinessPlan.REASON_ABSTRACT_NO_CONCRETE_SUBCLASS));
    }

    // ---- FlakinessJson ----

    @Test
    public void testParsesRefsAndRoundTripsPlan() {
        String refsJson = """
            { "mergeBase": "abc123",
              "refs": [
                { "source": "changed-file", "path": "server/src/test/java/org/elasticsearch/FooTests.java" },
                { "source": "unmute", "className": "org.foo.BarTests", "method": "test {yaml=x/y}" },
                { "source": "explicit", "spec": "org.foo.BazTests.testX" } ] }
            """;
        FlakinessJson.RefsFile refs = FlakinessJson.parseRefs(refsJson);
        assertThat(refs.mergeBase(), equalTo("abc123"));
        assertThat(refs.refs(), hasSize(3));
        assertThat(refs.refs().get(0).path(), equalTo("server/src/test/java/org/elasticsearch/FooTests.java"));
        assertThat(refs.refs().get(1).method(), equalTo("test {yaml=x/y}"));
        assertThat(refs.refs().get(2).spec(), equalTo("org.foo.BazTests.testX"));

        // The per-project targets envelope (the resolve -> scan hand-off) is pinned by
        // FlakinessPerProjectJsonTests, including that its java.nio.file.Path fields survive the trip.

        FlakinessPlan plan = FlakinessPlan.buildFailed("precompile");
        String pjson = FlakinessJson.writePlan(plan);
        assertThat(pjson.contains("\"buildFailed\" : true"), is(true));
        assertThat(pjson.contains("\"reason\" : \"precompile\""), is(true));
    }

    @Test
    public void testParseSpecForms() {
        assertThat(RefResolver.parseSpec("org.foo.BarTests").className(), equalTo("org.foo.BarTests"));
        assertThat(RefResolver.parseSpec("org.foo.BarTests").method(), is(nullValue()));
        assertThat(RefResolver.parseSpec("org.foo.BarTests.testX").method(), equalTo("testX"));
        assertThat(RefResolver.parseSpec("org.foo.YamlIT.test {yaml=a/b}").className(), equalTo("org.foo.YamlIT"));
        assertThat(RefResolver.parseSpec("org.foo.YamlIT.test {yaml=a/b}").method(), equalTo("test {yaml=a/b}"));
    }

    // ---- fixtures ----

    private List<ProjectInfo> fixtureProjects(Path repo) {
        return List.of(
            new ProjectInfo(
                ":server",
                repo.resolve("server"),
                List.of(ssi(repo, "server", "test"), ssi(repo, "server", "internalClusterTest"))
            ),
            new ProjectInfo(
                ":x-pack:plugin:esql",
                repo.resolve("x-pack/plugin/esql"),
                List.of(
                    ssi(repo, "x-pack/plugin/esql", "test"),
                    ssi(repo, "x-pack/plugin/esql", "yamlRestTest")
                )
            ),
            new ProjectInfo(":qa:rolling", repo.resolve("qa/rolling"), List.of(ssi(repo, "qa/rolling", "javaRestTest")))
        );
    }

    /**
     * A single-project resolver with the {@code Test}-task facts the real build would report for it: an
     * ordinary project has an enabled bare task, while {@code :qa:rolling} mirrors
     * {@code elasticsearch.bwc-test} - a <em>disabled</em> bare {@code javaRestTest} plus differently named
     * tasks pointed at the same source-set output.
     */
    private static RefResolver resolver(Path repo, ProjectInfo p) {
        List<TestTaskInfo> tasks = new ArrayList<>();
        boolean bwcProject = p.projectPath().equals(":qa:rolling");
        for (SourceSetInfo ss : p.sourceSets()) {
            tasks.add(testTask(p.projectPath(), ss.name(), bwcProject == false, ss.outputDir()));
            if (bwcProject) {
                tasks.add(testTask(p.projectPath(), "bcUpgradeTest", true, ss.outputDir()));
                tasks.add(testTask(p.projectPath(), "v9.5.1#bwcTest", true, ss.outputDir()));
                tasks.add(testTask(p.projectPath(), "v9.6.0#bwcTest", true, ss.outputDir()));
            }
        }
        return new RefResolver(repo, p, tasks, TestTaskSelector.DEFAULT_TASK_CAP);
    }

    /**
     * Resolve refs across several projects exactly the way the pipeline does: one single-project resolver per
     * project, one ref at a time so each target keeps its ref index, then the real global fold. That fold is
     * what turns per-project "not mine" verdicts into a single {@code unresolved} entry, so asserting on the
     * merged result is asserting on what the plan will actually say.
     *
     * @see FlakinessResolveProjectTask#resolve()
     * @see FlakinessScanTask#scan()
     */
    private static FlakinessJson.BaseTargetsFile resolveAcross(Path repo, List<ProjectInfo> projects, List<FlakinessRef> refs) {
        List<FlakinessJson.ProjectTargetsFile> perProject = new ArrayList<>();
        for (ProjectInfo p : projects) {
            RefResolver resolver = resolver(repo, p);
            List<FlakinessJson.RefTarget> resolved = new ArrayList<>();
            for (int i = 0; i < refs.size(); i++) {
                int refIndex = i;
                resolver.resolve(refs.get(i)).ifPresent(t -> resolved.add(new FlakinessJson.RefTarget(refIndex, t)));
            }
            perProject.add(new FlakinessJson.ProjectTargetsFile(p.projectPath(), resolved, List.of(), List.of()));
        }
        return FlakinessTargets.merge(refs, perProject);
    }

    private static TestTaskInfo testTask(String projectPath, String name, boolean enabled, Path classesDir) {
        return new TestTaskInfo(name, projectPath + ":" + name, enabled, List.of(classesDir));
    }

    /**
     * Build an authoritative {@link SourceSetInfo} matching the conventional ES layout under the fixture
     * repo. The resolver works off these real dirs (not a {@code src/<ss>/java} assumption).
     */
    private static SourceSetInfo ssi(Path repo, String projRel, String ssName) {
        Path base = repo.resolve(projRel).resolve("src").resolve(ssName);
        List<Path> javaSrcDirs = List.of(base.resolve("java"));
        List<Path> resourceSrcDirs = List.of(base.resolve("resources"));
        Path outputDir = repo.resolve(projRel).resolve("build/classes/java/" + ssName);
        return new SourceSetInfo(ssName, javaSrcDirs, resourceSrcDirs, outputDir);
    }

    private static BaseTarget planTarget(
        String project,
        String sourceSet,
        String kind,
        String fqcn,
        String suitePath,
        List<String> runnableTasks,
        int candidateTasks,
        String skipReason
    ) {
        return new BaseTarget(project, sourceSet, kind, fqcn, suitePath, null, runnableTasks, candidateTasks, skipReason);
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

    private static BaseTarget findByFqcn(List<BaseTarget> targets, String fqcn) {
        return targets.stream().filter(t -> fqcn.equals(t.fqcn())).findFirst().orElseThrow();
    }

    private void touch(Path repo, String relPath) throws IOException {
        Path f = repo.resolve(relPath);
        Files.createDirectories(f.getParent());
        Files.writeString(f, "// fixture\n");
    }

    private static void writeClass(Path root, String internalName, String superInternal, boolean isAbstract) throws IOException {
        ClassWriter cw = new ClassWriter(0);
        int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | (isAbstract ? Opcodes.ACC_ABSTRACT : 0);
        cw.visit(Opcodes.V17, access, internalName, null, superInternal, null);
        cw.visitEnd();
        Path out = root.resolve(internalName + ".class");
        Files.createDirectories(out.getParent());
        Files.write(out, cw.toByteArray());
    }
}
