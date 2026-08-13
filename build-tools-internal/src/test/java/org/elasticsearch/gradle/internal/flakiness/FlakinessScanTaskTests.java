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
            new BaseTarget(":a", "test", "test", "org.foo.ATests", null, null, false, ":a:compileTestJava", "/out/a/test"),
            // Same output dir as above -> collapsed.
            new BaseTarget(":a", "test", "test", "org.foo.A2Tests", null, null, false, ":a:compileTestJava", "/out/a/test"),
            new BaseTarget(
                ":b",
                "internalClusterTest",
                "internalClusterTest",
                "org.foo.BIT",
                null,
                null,
                false,
                ":b:compileInternalClusterTestJava",
                "/out/b/ict"
            ),
            // yaml suite -> not bytecode-enriched, no scan dir.
            new BaseTarget(":c", "yamlRestTest", "yamlRestTestSuite", null, "x/10_foo", null, false, ":c:compileYamlRestTestJava", "/out/c"),
            // bwc -> excluded even though it is a java kind.
            new BaseTarget(":d", "test", "test", "org.foo.DTests", null, null, true, ":d:compileTestJava", "/out/d/test")
        );

        assertThat(FlakinessScanTask.scanDirs(targets), containsInAnyOrder(Path.of("/out/a/test"), Path.of("/out/b/ict")));
    }
}
