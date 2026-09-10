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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for the per-project source-set dispositions the task reports. The rest of the task is Gradle
 * wiring, covered by {@code FlakinessProjectResolvePluginFuncTest}; the disposition derivation is a pure
 * function of the captured model, so it is tested directly here.
 */
public class FlakinessResolveProjectTaskTests {

    /**
     * Every candidate source set reports a disposition, {@code yamlRestTest} included. This is what lets the
     * scan step re-home a concrete subclass it found in a yamlRestTest output: the abstract base is a
     * java-kind target in some other source set, and this project's {@code yamlRestTest} task is what really
     * runs the subclass. Gating this on {@link Kinds#BYTECODE_ENRICHED} - whether a source set can be
     * <em>expanded</em> - left those outputs scanned but unclaimed, so every subclass found in one became a
     * {@code subclass-outside-target-output} skip.
     */
    @Test
    public void testEveryCandidateSourceSetReportsADispositionIncludingYamlRestTest() {
        FlakinessJson.ProjectModel model = model(
            List.of(sourceSet(Kinds.SS_TEST), sourceSet(Kinds.SS_YAML_REST_TEST)),
            List.of(bareTask(Kinds.SS_TEST), bareTask(Kinds.SS_YAML_REST_TEST))
        );

        List<SourceSetDisposition> dispositions = FlakinessResolveProjectTask.dispositionsOf(model, 2);

        assertThat(dispositions.stream().map(SourceSetDisposition::sourceSet).toList(), containsInAnyOrder("test", "yamlRestTest"));
    }

    /** The yamlRestTest disposition has to be runnable, or re-homing only swaps one skip reason for another. */
    @Test
    public void testYamlRestTestDispositionCarriesItsRunnerTaskAndKind() {
        FlakinessJson.ProjectModel model = model(
            List.of(sourceSet(Kinds.SS_YAML_REST_TEST)),
            List.of(bareTask(Kinds.SS_YAML_REST_TEST))
        );

        SourceSetDisposition yaml = FlakinessResolveProjectTask.dispositionsOf(model, 2).get(0);

        assertThat(yaml.runnable(), is(true));
        assertThat(yaml.runnableTasks(), contains(":p:yamlRestTest"));
        assertThat(yaml.skipReason(), is(nullValue()));
        // The wire kind a yaml runner class resolves to; PlanBuilder stamps it onto the re-homed entry.
        assertThat(yaml.kind(), equalTo(Kinds.YAML_REST_TEST_RUNNER));
    }

    /**
     * A source set no enabled task points at still reports, carrying its own reason. The scan step needs the
     * entry either way: it is what lets a re-homed subclass say "this project cannot run it here" rather than
     * being attributed to the base's tasks, which do not contain it.
     */
    @Test
    public void testASourceSetWithNoOverlappingTaskStillReportsWithItsReason() {
        FlakinessJson.ProjectModel model = model(
            List.of(sourceSet(Kinds.SS_TEST)),
            List.of(new TestTaskInfo("test", ":p:test", true, List.of(Path.of("/p/build/classes/java/elsewhere"))))
        );

        SourceSetDisposition test = FlakinessResolveProjectTask.dispositionsOf(model, 2).get(0);

        assertThat(test.sourceSet(), equalTo("test"));
        assertThat(test.runnable(), is(false));
        assertThat(test.skipReason(), equalTo(TestTaskSelector.REASON_NO_RUNNABLE_TASK));
    }

    // ---- fixtures ----

    private static FlakinessJson.ProjectModel model(List<SourceSetInfo> sourceSets, List<TestTaskInfo> testTasks) {
        return new FlakinessJson.ProjectModel(":p", Path.of("/p"), sourceSets, testTasks, List.of(), false);
    }

    private static SourceSetInfo sourceSet(String name) {
        Path base = Path.of("/p/src").resolve(name);
        return new SourceSetInfo(name, List.of(base.resolve("java")), List.of(base.resolve("resources")), outputDir(name));
    }

    /** The conventional bare task for a source set: enabled, pointed at that source set's own output. */
    private static TestTaskInfo bareTask(String sourceSet) {
        return new TestTaskInfo(sourceSet, ":p:" + sourceSet, true, List.of(outputDir(sourceSet)));
    }

    private static Path outputDir(String sourceSet) {
        return Path.of("/p/build/classes/java").resolve(sourceSet);
    }
}
