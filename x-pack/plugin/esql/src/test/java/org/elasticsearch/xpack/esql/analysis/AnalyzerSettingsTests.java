/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AnalyzerSettingsTests extends ESTestCase {
    /**
     * The row maximum takes a size in its range or the value that removes it, and refuses the rest naming the rule.
     */
    public void testRowMaximumParsing() {
        assertThat(parse("10000"), equalTo(10_000));
        assertThat(parse("1"), equalTo(1));
        assertThat(parse(Integer.toString(AnalyzerSettings.LARGEST_MAXIMUM)), equalTo(AnalyzerSettings.LARGEST_MAXIMUM));
        assertThat(parse("-1"), equalTo(AnalyzerSettings.NO_MAXIMUM));
        for (String refused : new String[] { "0", "-2", Integer.toString(AnalyzerSettings.LARGEST_MAXIMUM + 1), "many" }) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> parse(refused));
            assertThat(e.getMessage(), containsString("esql.query.result_truncation_max_size"));
        }
    }

    private static int parse(String value) {
        return AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.get(
            Settings.builder().put(AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getKey(), value).build()
        );
    }

    /**
     * A sort nothing else bounds, in a branch of a union, takes the maximum, or the largest one when there is none.
     */
    public void testUnboundedSortLimit() {
        assertThat(EsqlTestUtils.configurationWithRowMaximum(5_000).unboundedSortLimit(), equalTo(5_000));
        assertThat(
            EsqlTestUtils.configurationWithRowMaximum(AnalyzerSettings.NO_MAXIMUM).unboundedSortLimit(),
            equalTo(AnalyzerSettings.LARGEST_MAXIMUM)
        );
    }
}
