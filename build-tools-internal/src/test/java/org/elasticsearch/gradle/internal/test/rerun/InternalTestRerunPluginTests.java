/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rerun;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class InternalTestRerunPluginTests {

    private final Logger logger = Logging.getLogger(InternalTestRerunPluginTests.class);

    @Test
    public void testPlainMethodYieldsSinglePattern() {
        Set<String> patterns = InternalTestRerunPlugin.buildMethodExcludePatterns(List.of("org.es.FooTest#testBar"), logger);

        assertThat(patterns, contains("org.es.FooTest.testBar"));
    }

    @Test
    public void testParameterizedMethodAlsoYieldsBarePattern() {
        Set<String> patterns = InternalTestRerunPlugin.buildMethodExcludePatterns(
            List.of("org.es.FooTest#testBar {upgradedNodes=0}"),
            logger
        );

        assertThat(patterns, contains("org.es.FooTest.testBar {upgradedNodes=0}", "org.es.FooTest.testBar"));
    }

    @Test
    public void testYamlSuiteParametersYieldBothPatterns() {
        Set<String> patterns = InternalTestRerunPlugin.buildMethodExcludePatterns(
            List.of("org.es.ClientYamlTestSuiteIT#test {yaml=analysis-common/30_tokenizers/letter}"),
            logger
        );

        assertThat(
            patterns,
            contains("org.es.ClientYamlTestSuiteIT.test {yaml=analysis-common/30_tokenizers/letter}", "org.es.ClientYamlTestSuiteIT.test")
        );
    }

    @Test
    public void testBarePatternIsSharedAcrossParametersOfTheSameMethod() {
        Set<String> patterns = InternalTestRerunPlugin.buildMethodExcludePatterns(
            List.of("org.es.FooTest#testBar {upgradedNodes=0}", "org.es.FooTest#testBar {upgradedNodes=1}"),
            logger
        );

        assertThat(
            patterns,
            contains("org.es.FooTest.testBar {upgradedNodes=0}", "org.es.FooTest.testBar", "org.es.FooTest.testBar {upgradedNodes=1}")
        );
    }

    @Test
    public void testDuplicateReferencesCollapse() {
        Set<String> patterns = InternalTestRerunPlugin.buildMethodExcludePatterns(
            List.of("org.es.FooTest#testBar", "org.es.FooTest#testBar"),
            logger
        );

        assertThat(patterns, contains("org.es.FooTest.testBar"));
    }

    @Test
    public void testMalformedReferencesAreSkipped() {
        Set<String> patterns = InternalTestRerunPlugin.buildMethodExcludePatterns(
            List.of("org.es.FooTest", "org.es.FooTest#testBar"),
            logger
        );

        assertThat(patterns, contains("org.es.FooTest.testBar"));
    }

    @Test
    public void testEmptyInputYieldsNoPatterns() {
        assertThat(InternalTestRerunPlugin.buildMethodExcludePatterns(List.of(), logger), empty());
    }

    @Test
    public void testBraceWithoutLeadingSpaceIsTreatedAsPlainMethodName() {
        // Only the randomized runner's " {" separator marks parameters. A brace anywhere else is part of the method name.
        Set<String> patterns = InternalTestRerunPlugin.buildMethodExcludePatterns(List.of("org.es.FooTest#testBar{x}"), logger);

        assertThat(patterns.size(), equalTo(1));
        assertThat(patterns, contains("org.es.FooTest.testBar{x}"));
    }
}
