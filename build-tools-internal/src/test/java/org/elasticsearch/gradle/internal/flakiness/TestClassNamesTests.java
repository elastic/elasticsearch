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

/**
 * Unit tests for the "could a {@code Test} task run this class?" filter. Every case here is a real name taken
 * from the repo, because the whole point of the check is to match what Elasticsearch actually does rather than
 * an invented convention.
 */
public class TestClassNamesTests {

    @Test
    public void testAcceptsTheConventionalTestSuffixes() {
        assertThat(TestClassNames.isRunnableTestClass("org.elasticsearch.dissect.DissectParserTests"), is(true));
        assertThat(TestClassNames.isRunnableTestClass("org.elasticsearch.upgrades.FullClusterRestartIT"), is(true));
    }

    /**
     * {@code TestCase} is normally an abstract base, but a few concrete runnable tests use it - this one has
     * three test methods and nothing extends it. Abstract classes never reach this check (they are expanded,
     * not run), so accepting the suffix costs nothing while rejecting it would silently drop a real test.
     */
    @Test
    public void testAcceptsAConcreteTestCaseBecauseSomeAreRealTests() {
        assertThat(
            TestClassNames.isRunnableTestClass("org.elasticsearch.lucene.RollingUpgradeLuceneIndexCompatibilityTestCase"),
            is(true)
        );
    }

    /**
     * The helpers that share a source set with the tests. Left unfiltered, a change to one of these resolves to
     * a target and emits {@code --tests <helper>}, which matches nothing and reads downstream as a hang.
     */
    @Test
    public void testRejectsHelpersThatLiveInTestSourceSets() {
        assertThat(TestClassNames.isRunnableTestClass("org.elasticsearch.repositories.gcs.WebProxyServer"), is(false));
        assertThat(TestClassNames.isRunnableTestClass("org.elasticsearch.test.apmintegration.OtlpLogsParser"), is(false));
        assertThat(TestClassNames.isRunnableTestClass("org.elasticsearch.xpack.security.LocalStateSecurity"), is(false));
        assertThat(TestClassNames.isRunnableTestClass("org.elasticsearch.xpack.esql.action.SimplePauseFieldPlugin"), is(false));
    }

    /**
     * Inner and anonymous classes are concrete in bytecode, so subclass expansion surfaces them, but
     * {@code --tests Foo$1} addresses nothing. They also cannot arrive from a source file or a
     * {@code muted-tests.yml} entry, so rejecting them outright loses nothing.
     */
    @Test
    public void testRejectsInnerAndAnonymousClassesEvenWithATestSuffix() {
        assertThat(TestClassNames.isRunnableTestClass("org.elasticsearch.cluster.serialization.DiffableTests$1"), is(false));
        // Suffix looks right, but it is still an inner class Gradle's --tests filter cannot address.
        assertThat(TestClassNames.isRunnableTestClass("org.foo.OuterTests$InnerTests"), is(false));
    }

    @Test
    public void testRejectsNullAndBlank() {
        assertThat(TestClassNames.isRunnableTestClass(null), is(false));
        assertThat(TestClassNames.isRunnableTestClass(""), is(false));
        assertThat(TestClassNames.isRunnableTestClass("   "), is(false));
    }

    /** A suffix must end the name, not merely appear in it. */
    @Test
    public void testSuffixMustBeASuffix() {
        assertThat(TestClassNames.isRunnableTestClass("org.foo.TestsHelper"), is(false));
        assertThat(TestClassNames.isRunnableTestClass("org.foo.ITSupport"), is(false));
    }
}
