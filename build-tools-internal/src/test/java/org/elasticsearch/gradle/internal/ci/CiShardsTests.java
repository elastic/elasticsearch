/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class CiShardsTests {

    /** Pins the hash so a JDK or refactoring change cannot silently move every default-placed task. */
    @Test
    public void testBucketIsStable() {
        assertThat(CiShards.bucket(":server:test", 12), equalTo(11));
        // ESClientYamlSuiteTestCaseTests#testShardOfMatchesTheBuild pins the same value for the yaml runner's copy
        assertThat(CiShards.bucket("index/10_basic", 7), equalTo(5));
        assertThat(CiShards.bucket("org.elasticsearch.FooIT", 1), equalTo(0));
    }

    @Test
    public void testBucketsAreRoughlyEven() {
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < 12_000; i++) {
            counts.merge(CiShards.bucket("org.elasticsearch.pkg" + (i % 37) + ".Some" + i + "IT", 12), 1, Integer::sum);
        }
        assertThat(counts.size(), equalTo(12));
        counts.values().forEach(c -> {
            assertThat(c, greaterThan(800));
            assertThat(c, lessThan(1200));
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsNonPositiveCount() {
        CiShards.bucket("x", 0);
    }

    @Test
    public void testNestedClassesFollowTheirTopLevelClass() {
        assertThat(CiShards.topLevelClassName("org/foo/BarIT.class"), equalTo("org.foo.BarIT"));
        assertThat(CiShards.topLevelClassName("org/foo/BarIT$Inner$1.class"), equalTo("org.foo.BarIT"));
        assertThat(CiShards.topLevelClassName("org\\foo\\BarIT$Inner.class"), equalTo("org.foo.BarIT"));
        assertThat(CiShards.topLevelClassName("Top.class"), equalTo("Top"));
    }
}
