/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.junit.runner;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class IterationSeedsTests extends RandomizedTest {

    public void testSeedOfATestWithoutParameters() {
        assertThat(IterationSeeds.seedOf("testFoo {seed=[A1:B2]}"), equalTo("A1:B2"));
    }

    public void testSeedOfATestWithParameters() {
        assertThat(IterationSeeds.seedOf("testFoo {p=a seed=[A1:B2]}"), equalTo("A1:B2"));
        assertThat(IterationSeeds.seedOf("testFoo {p=a q=b seed=[A1:B2]}"), equalTo("A1:B2"));
    }

    public void testNoSeed() {
        assertThat(IterationSeeds.seedOf("testFoo"), nullValue());
        assertThat(IterationSeeds.seedOf("testFoo {p=a}"), nullValue());
        assertThat(IterationSeeds.seedOf(null), nullValue());
    }

    public void testSeedWithoutIterationSeedIsNotASeed() {
        assertThat(IterationSeeds.seedOf("testFoo {seed=[A1]}"), nullValue());
        assertThat(IterationSeeds.withoutSeed("testFoo {seed=[A1]}"), equalTo("testFoo {seed=[A1]}"));
    }

    public void testWithoutSeedOfATestWithoutParameters() {
        assertThat(IterationSeeds.withoutSeed("testFoo {seed=[A1:B2]}"), equalTo("testFoo"));
    }

    public void testWithoutSeedKeepsTheParameters() {
        assertThat(IterationSeeds.withoutSeed("testFoo {p=a seed=[A1:B2]}"), equalTo("testFoo {p=a}"));
        assertThat(IterationSeeds.withoutSeed("testFoo {p=a q=b seed=[A1:B2]}"), equalTo("testFoo {p=a q=b}"));
    }

    public void testWithoutSeedLeavesOtherNamesAlone() {
        assertThat(IterationSeeds.withoutSeed("testFoo"), equalTo("testFoo"));
        assertThat(IterationSeeds.withoutSeed("testFoo {p=a}"), equalTo("testFoo {p=a}"));
        assertThat(IterationSeeds.withoutSeed(null), nullValue());
    }

    /** The ` {}` and the seed-like values belong to the parameters of the test, they are not what the runner adds. */
    public void testOnlyTheTrailingSeedIsRemoved() {
        assertThat(IterationSeeds.withoutSeed("testFoo {q=match {}}"), equalTo("testFoo {q=match {}}"));
        assertThat(IterationSeeds.withoutSeed("testFoo {q=match {} seed=[A1:B2]}"), equalTo("testFoo {q=match {}}"));
        assertThat(IterationSeeds.withoutSeed("testFoo {}"), equalTo("testFoo {}"));
        assertThat(IterationSeeds.withoutSeed("testFoo {p=seed=[A1:B2] q=b}"), equalTo("testFoo {p=seed=[A1:B2] q=b}"));
        assertThat(IterationSeeds.withoutSeed("testFoo {p=seed=[A1:B2] q=b seed=[C3:D4]}"), equalTo("testFoo {p=seed=[A1:B2] q=b}"));
    }

    public void testSeedLikeParameterIsNotTheSeedOfTheIteration() {
        assertThat(IterationSeeds.seedOf("testFoo {p=seed=[A1:B2] q=b}"), nullValue());
        assertThat(IterationSeeds.seedOf("testFoo {p=seed=[A1:B2] q=b seed=[C3:D4]}"), equalTo("C3:D4"));
    }
}
