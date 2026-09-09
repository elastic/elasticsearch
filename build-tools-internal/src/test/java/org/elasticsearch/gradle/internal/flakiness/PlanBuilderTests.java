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
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for the step that turns resolved targets into the plan the runner executes: flattening abstract
 * bases into their concrete subclasses using the bytecode scan, and reporting what it had to skip or cap.
 *
 * <p>Only the abstract-flattening tests need real bytecode. The rest pass {@link #noBytecode()}, which states
 * that no class-hierarchy fact is involved in the behaviour under test.
 */
public class PlanBuilderTests {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    /** An abstract base carries no test methods of its own, so it must never survive as a run entry. */
    @Test
    public void testFlattensAnAbstractTargetIntoOneRunEntryPerConcreteSubclass() throws IOException {
        List<BaseTarget> targets = List.of(javaTarget(":a", "com.example.AbstractFooTests"));

        FlakinessPlan plan = PlanBuilder.build(targets, List.of(), scanAbstractBaseWithTwoSubclasses(), 5, 1);

        List<String> fqcns = plan.entries().stream().map(PlanEntry::fqcn).toList();
        assertThat(fqcns, containsInAnyOrder("com.example.BarTests", "com.example.BazTests"));
        assertThat(fqcns, not(hasItem("com.example.AbstractFooTests")));
        // Each entry records where it came from, so the report can explain why a class nobody touched is run.
        assertThat(
            plan.entries().stream().map(PlanEntry::expandedFrom).distinct().toList(),
            contains("com.example.AbstractFooTests")
        );
        assertThat(plan.entries().stream().map(PlanEntry::disposition).distinct().toList(), contains("run"));
    }

    @Test
    public void testRecordsAnExpansionForEachFlattenedAbstractTarget() throws IOException {
        List<BaseTarget> targets = List.of(javaTarget(":a", "com.example.AbstractFooTests"));

        FlakinessPlan plan = PlanBuilder.build(targets, List.of(), scanAbstractBaseWithTwoSubclasses(), 5, 1);

        assertThat(plan.expansions(), hasSize(1));
        assertThat(plan.expansions().get(0).abstractFqcn(), equalTo("com.example.AbstractFooTests"));
        assertThat(plan.expansions().get(0).ran(), equalTo(2));
        assertThat(plan.expansions().get(0).total(), equalTo(2));
    }

    /**
     * An abstract base nothing extends is a real gap - the ref pointed at something unrunnable - so it is
     * surfaced as unresolved rather than silently dropped from the plan.
     */
    @Test
    public void testAnAbstractTargetWithNoConcreteSubclassIsReportedUnresolved() throws IOException {
        Path classes = tmp.newFolder("classes").toPath();
        writeClass(classes, "com/example/LoneAbstractTests", "java/lang/Object", true);
        ClassHierarchyScanner scanner = ClassHierarchyScanner.scan(List.of(classes));
        List<BaseTarget> targets = List.of(javaTarget(":d", "com.example.LoneAbstractTests"));

        FlakinessPlan plan = PlanBuilder.build(targets, List.of(), scanner, 5, 1);

        assertThat(plan.unresolved(), hasSize(1));
        assertThat(plan.unresolved().get(0).reason(), equalTo(FlakinessPlan.REASON_ABSTRACT_NO_CONCRETE_SUBCLASS));
    }

    /** A target no task can run stays in the plan as a skip, carrying the precise reason it cannot run. */
    @Test
    public void testATargetWithNothingRunnableBecomesASkipCarryingItsReason() {
        BaseTarget packaging = unrunnable(":b", "org.foo.SomeTests", TestTaskSelector.REASON_REQUIRES_PACKAGING_HOST);

        FlakinessPlan plan = PlanBuilder.build(List.of(packaging), List.of(), noBytecode(), 5, 1);

        PlanEntry entry = onlyEntry(plan);
        assertThat(entry.disposition(), equalTo("skip"));
        assertThat(entry.reason(), equalTo("requires-packaging-host"));
        assertThat(entry.runnableTasks(), is(empty()));
    }

    /** A yaml suite is addressed by suite path, so there is no class to expand and nothing to enrich. */
    @Test
    public void testYamlSuiteTargetsPassThroughUnenrichedWithTheirTaskPaths() {
        FlakinessPlan plan = PlanBuilder.build(List.of(yamlSuiteTarget(":c", "esql/10_foo")), List.of(), noBytecode(), 5, 1);

        PlanEntry entry = onlyEntry(plan);
        assertThat(entry.disposition(), equalTo("run"));
        assertThat(entry.suitePath(), equalTo("esql/10_foo"));
        assertThat(entry.fqcn(), is(nullValue()));
        assertThat(entry.runnableTasks(), contains(":c:yamlRestTest"));
        assertThat(plan.expansions(), is(empty()));
    }

    /**
     * Only a fan-out the cap actually truncated is worth reporting; a 1-of-1 selection would be noise in a
     * report a human has to read.
     */
    @Test
    public void testOnlyCappedTaskFanOutsAreReported() {
        List<BaseTarget> targets = List.of(
            javaTarget(":a", "org.foo.PlainTests"),
            cappedFanOut(":e", "org.foo.SomeIT", List.of(":e:v9.6.0#bwcTest"), 67)
        );

        FlakinessPlan plan = PlanBuilder.build(targets, List.of(), noBytecode(), 5, 1);

        assertThat(plan.taskSelections(), hasSize(1));
        assertThat(plan.taskSelections().get(0).gradleProject(), equalTo(":e"));
        assertThat(plan.taskSelections().get(0).total(), equalTo(67));
        assertThat(plan.taskSelections().get(0).cap(), equalTo(1));
        assertThat(plan.taskSelections().get(0).selected(), contains(":e:v9.6.0#bwcTest"));
    }

    // ---- fixtures ----

    /**
     * A scanner that visited no bytecode: every class is unknown, so {@code expand} passes the fqcn straight
     * through as concrete. That is exactly the right input for behaviour that has nothing to do with the
     * class hierarchy, and it says so at the call site.
     */
    private static ClassHierarchyScanner noBytecode() {
        return ClassHierarchyScanner.scan(List.of());
    }

    private ClassHierarchyScanner scanAbstractBaseWithTwoSubclasses() throws IOException {
        Path classes = tmp.newFolder("classes").toPath();
        writeClass(classes, "com/example/AbstractFooTests", "java/lang/Object", true);
        writeClass(classes, "com/example/BarTests", "com/example/AbstractFooTests", false);
        writeClass(classes, "com/example/BazTests", "com/example/AbstractFooTests", false);
        return ClassHierarchyScanner.scan(List.of(classes));
    }

    private static PlanEntry onlyEntry(FlakinessPlan plan) {
        assertThat(plan.entries(), hasSize(1));
        return plan.entries().get(0);
    }

    private static BaseTarget javaTarget(String project, String fqcn) {
        return new BaseTarget(project, "test", "test", fqcn, null, null, List.of(project + ":test"), 1, null);
    }

    private static BaseTarget unrunnable(String project, String fqcn, String skipReason) {
        return new BaseTarget(project, "test", "test", fqcn, null, null, List.of(), 12, skipReason);
    }

    private static BaseTarget yamlSuiteTarget(String project, String suitePath) {
        String sourceSet = Kinds.SS_YAML_REST_TEST;
        List<String> tasks = List.of(project + ":" + sourceSet);
        return new BaseTarget(project, sourceSet, Kinds.YAML_REST_TEST_SUITE, null, suitePath, null, tasks, 1, null);
    }

    private static BaseTarget cappedFanOut(String project, String fqcn, List<String> selected, int candidateTasks) {
        String sourceSet = Kinds.SS_JAVA_REST_TEST;
        return new BaseTarget(project, sourceSet, Kinds.JAVA_REST_TEST, fqcn, null, null, selected, candidateTasks, null);
    }

    // Duplicated in ClassHierarchyScannerTests; local ASM builders are the convention here (see
    // ExtractForeignApiTaskTests). A real nested class would carry a '$' in its name, which
    // TestClassNames.isRunnableTestClass rejects, so generated bytecode is the only usable fixture.
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
