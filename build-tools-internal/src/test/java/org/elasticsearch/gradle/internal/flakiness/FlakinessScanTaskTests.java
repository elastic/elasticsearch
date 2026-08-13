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
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Unit tests for {@link FlakinessScanTask}'s task-specific pure logic (choosing which compiled-output
 * directories to ASM-scan). The end-to-end scan (compiling, reading the base-targets file, flattening
 * abstract bases) is exercised by {@code FlakinessResolvePluginFuncTest}.
 */
public class FlakinessScanTaskTests {

    @Test
    public void testScansOnlyBytecodeEnrichedRunnableOutputDirs() {
        List<BaseTarget> targets = List.of(
            runnable(":a", "test", "test", "org.foo.ATests", null, "/out/a/test"),
            // Same output dir as above -> collapsed.
            runnable(":a", "test", "test", "org.foo.A2Tests", null, "/out/a/test"),
            runnable(":b", "internalClusterTest", "internalClusterTest", "org.foo.BIT", null, "/out/b/ict"),
            // yaml suite -> not bytecode-enriched, no scan dir.
            runnable(":c", "yamlRestTest", "yamlRestTestSuite", null, "x/10_foo", "/out/c"),
            // Nothing runnable -> excluded even though it is a java kind.
            skipped(":d", "test", "test", "org.foo.DTests", "/out/d/test")
        );

        assertThat(FlakinessScanTask.scanDirs(targets), containsInAnyOrder(Path.of("/out/a/test"), Path.of("/out/b/ict")));
    }

    private static BaseTarget runnable(String project, String sourceSet, String kind, String fqcn, String suitePath, String outputDir) {
        return new BaseTarget(
            project,
            sourceSet,
            kind,
            fqcn,
            suitePath,
            null,
            project + ":compileX",
            outputDir,
            List.of(project + ":" + sourceSet),
            1,
            null
        );
    }

    private static BaseTarget skipped(String project, String sourceSet, String kind, String fqcn, String outputDir) {
        return new BaseTarget(
            project,
            sourceSet,
            kind,
            fqcn,
            null,
            null,
            project + ":compileX",
            outputDir,
            List.of(),
            0,
            TestTaskSelector.REASON_NO_RUNNABLE_TASK
        );
    }
}
