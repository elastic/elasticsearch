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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for the project-path to file-name mapping every project writes its share of the answer under.
 * The plugin's registration behaviour is covered by {@code FlakinessProjectResolvePluginFuncTest}, which runs
 * a real Gradle build; only the pure mapping is unit-testable.
 */
public class FlakinessProjectResolvePluginTests {

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
}
