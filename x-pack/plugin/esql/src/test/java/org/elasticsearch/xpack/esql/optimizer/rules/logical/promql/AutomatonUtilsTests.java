/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.PromqlFeatures;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.AutomatonUtils.PatternFragment;
import org.junit.BeforeClass;

import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.AutomatonUtils.PatternFragment.Type.EXACT;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.AutomatonUtils.PatternFragment.Type.PREFIX;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.AutomatonUtils.PatternFragment.Type.PROPER_PREFIX;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.AutomatonUtils.PatternFragment.Type.PROPER_SUFFIX;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.AutomatonUtils.PatternFragment.Type.REGEX;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.AutomatonUtils.PatternFragment.Type.SUFFIX;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.AutomatonUtils.extractFragments;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assume.assumeTrue;

public class AutomatonUtilsTests extends ESTestCase {

    @BeforeClass
    public static void checkPromqlEnabled() {
        assumeTrue("requires snapshot build with promql feature enabled", PromqlFeatures.isEnabled());
    }

    public void testExtractFragments_ExactString() {
        // Single exact string (no wildcards)
        List<PatternFragment> fragments = extractFragments("api");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(1));
        assertFragment(fragments.get(0), EXACT, "api");
    }

    public void testExtractFragments_Prefix() {
        // Prefix pattern: prefix.*
        List<PatternFragment> fragments = extractFragments("prod-.*");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(1));
        assertFragment(fragments.get(0), PREFIX, "prod-");
    }

    public void testExtractFragments_Suffix() {
        // Suffix pattern: .*suffix
        List<PatternFragment> fragments = extractFragments(".*-prod");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(1));
        assertFragment(fragments.get(0), SUFFIX, "-prod");
    }

    public void testExtractFragments_MixedAlternation() {
        // Mixed alternation: prefix|exact|suffix
        List<PatternFragment> fragments = extractFragments("prod-.*|staging|.*-dev");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(3));

        Object[][] expected = { { PREFIX, "prod-" }, { EXACT, "staging" }, { SUFFIX, "-dev" } };

        assertFragments(fragments, expected);
    }

    public void testExtractFragments_ProperPrefixSuffixAlternation() {
        List<PatternFragment> fragments = extractFragments("prod-.+|.+-dev");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(2));

        Object[][] expected = { { PROPER_PREFIX, "prod-" }, { PROPER_SUFFIX, "-dev" } };

        assertFragments(fragments, expected);
    }

    public void testExtractFragments_HomogeneousExactAlternation() {
        // All exact values
        List<PatternFragment> fragments = extractFragments("api|web|service");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(3));

        Object[][] expected = { { EXACT, "api" }, { EXACT, "web" }, { EXACT, "service" } };

        assertFragments(fragments, expected);
    }

    public void testExtractFragments_HomogeneousPrefixAlternation() {
        // All prefixes
        List<PatternFragment> fragments = extractFragments("prod-.*|staging-.*|dev-.*");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(3));

        Object[][] expected = { { PREFIX, "prod-" }, { PREFIX, "staging-" }, { PREFIX, "dev-" } };

        assertFragments(fragments, expected);
    }

    public void testExtractFragments_HomogeneousSuffixAlternation() {
        // All suffixes
        List<PatternFragment> fragments = extractFragments(".*-prod|.*-staging|.*-dev");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(3));

        Object[][] expected = { { SUFFIX, "-prod" }, { SUFFIX, "-staging" }, { SUFFIX, "-dev" } };

        assertFragments(fragments, expected);
    }

    public void testExtractFragments_WithAnchors() {
        // Pattern with anchors should be normalized
        List<PatternFragment> fragments = extractFragments("^prod-.*|staging|.*-dev$");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(3));

        Object[][] expected = { { PREFIX, "prod-" }, { EXACT, "staging" }, { SUFFIX, "-dev" } };

        assertFragments(fragments, expected);
    }

    public void testExtractFragments_ContainsPattern() {
        // Contains pattern (.*substring.*) should return REGEX type
        List<PatternFragment> fragments = extractFragments(".*error.*");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(1));
        assertFragment(fragments.get(0), REGEX, ".*error.*");
    }

    public void testExtractFragments_ComplexRegex() {
        // Complex regex with character classes should return REGEX type
        List<PatternFragment> fragments = extractFragments("[0-9]+");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(1));
        assertFragment(fragments.get(0), REGEX, "[0-9]+");
    }

    public void testExtractFragments_MixedWithRegex() {
        // Mixed alternation with some REGEX fragments: exact|prefix|regex|suffix
        List<PatternFragment> fragments = extractFragments("api|prod-.*|[0-9]+|.*-dev");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(4));

        Object[][] expected = { { EXACT, "api" }, { PREFIX, "prod-" }, { REGEX, "[0-9]+" }, { SUFFIX, "-dev" } };

        assertFragments(fragments, expected);
    }

    public void testExtractFragments_ComplexPrefixPattern() {
        // Prefix with complex part should return REGEX
        List<PatternFragment> fragments = extractFragments("test[0-9]+.*");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(1));
        assertFragment(fragments.get(0), REGEX, "test[0-9]+.*");
    }

    public void testExtractFragments_ComplexSuffixPattern() {
        // Suffix with complex part should return REGEX
        List<PatternFragment> fragments = extractFragments(".*[a-z]{3}");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(1));
        assertFragment(fragments.get(0), REGEX, ".*[a-z]{3}");
    }

    public void testExtractFragments_NonMatchingPattern_NestedGroups() {
        // Nested groups should return null
        List<PatternFragment> fragments = extractFragments("(a(b|c))");

        assertThat(fragments, nullValue());
    }

    public void testExtractFragments_NonMatchingPattern_EscapedPipe() {
        // Escaped pipe should return null (too complex)
        List<PatternFragment> fragments = extractFragments("a\\|b");

        assertThat(fragments, nullValue());
    }

    public void testExtractFragments_RegexMetacharactersInAlternation() {
        // Pattern with regex metacharacters - should classify correctly
        List<PatternFragment> fragments = extractFragments("test.*|prod[0-9]");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(2));

        Object[][] expected = { { PREFIX, "test" }, { REGEX, "prod[0-9]" } };

        assertFragments(fragments, expected);
    }

    public void testExtractFragments_NullPattern() {
        // Null pattern should return null
        List<PatternFragment> fragments = extractFragments(null);

        assertThat(fragments, nullValue());
    }

    public void testExtractFragments_EmptyPattern() {
        // Empty pattern should return single EXACT fragment with empty value
        List<PatternFragment> fragments = extractFragments("");

        assertThat(fragments, notNullValue());
        assertThat(fragments, hasSize(1));
        assertFragment(fragments.get(0), EXACT, "");
    }

    public void testExtractFragments_TooManyAlternations() {
        // Create a pattern with more than MAX_IN_VALUES (256) alternations
        StringBuilder pattern = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            if (i > 0) {
                pattern.append("|");
            }
            pattern.append("a").append(i);
        }

        List<PatternFragment> fragments = extractFragments(pattern.toString());

        // Should return null because it exceeds MAX_IN_VALUES
        assertThat(fragments, nullValue());
    }

    private void assertFragment(PatternFragment fragment, PatternFragment.Type expectedType, String expectedValue) {
        assertThat(fragment.type(), equalTo(expectedType));
        assertThat(fragment.value(), equalTo(expectedValue));
    }

    private void assertFragments(List<PatternFragment> fragments, Object[][] expected) {
        assertThat(fragments, hasSize(expected.length));
        for (int i = 0; i < expected.length; i++) {
            PatternFragment.Type expectedType = (PatternFragment.Type) expected[i][0];
            String expectedValue = (String) expected[i][1];
            assertFragment(fragments.get(i), expectedType, expectedValue);
        }
    }
}
