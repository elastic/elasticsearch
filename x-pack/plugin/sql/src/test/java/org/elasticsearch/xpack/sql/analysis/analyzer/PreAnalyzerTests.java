/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.analysis.analyzer.PreAnalyzer.PreAnalysis;
import org.elasticsearch.xpack.sql.parser.SqlParser;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PreAnalyzerTests extends ESTestCase {

    private SqlParser parser = new SqlParser();
    private PreAnalyzer preAnalyzer = new PreAnalyzer();

    public void testBasicIndex() {
        LogicalPlan plan = parser.createStatement("SELECT * FROM index");
        PreAnalysis result = preAnalyzer.preAnalyze(plan);
        assertThat(plan.preAnalyzed(), is(true));
        assertThat(result.indices, hasSize(1));
        assertThat(result.indices.get(0).id().cluster(), nullValue());
        assertThat(result.indices.get(0).id().index(), is("index"));
    }

    public void testBasicIndexWithCatalog() {
        LogicalPlan plan = parser.createStatement("SELECT * FROM elastic:index");
        PreAnalysis result = preAnalyzer.preAnalyze(plan);
        assertThat(plan.preAnalyzed(), is(true));
        assertThat(result.indices, hasSize(1));
        assertThat(result.indices.get(0).id().cluster(), is("elastic"));
        assertThat(result.indices.get(0).id().index(), is("index"));
    }

    public void testWildIndexWithCatalog() {
        LogicalPlan plan = parser.createStatement("SELECT * FROM elastic:\"index*\"");
        PreAnalysis result = preAnalyzer.preAnalyze(plan);
        assertThat(plan.preAnalyzed(), is(true));
        assertThat(result.indices, hasSize(1));
        assertThat(result.indices.get(0).id().cluster(), is("elastic"));
        assertThat(result.indices.get(0).id().index(), is("index*"));
    }

    public void testQuotedIndex() {
        LogicalPlan plan = parser.createStatement("SELECT * FROM \"aaa\"");
        PreAnalysis result = preAnalyzer.preAnalyze(plan);
        assertThat(plan.preAnalyzed(), is(true));
        assertThat(result.indices, hasSize(1));
        assertThat(result.indices.get(0).id().cluster(), nullValue());
        assertThat(result.indices.get(0).id().index(), is("aaa"));
    }

    public void testQuotedCatalog() {
        LogicalPlan plan = parser.createStatement("SELECT * FROM \"elastic\":\"aaa\"");
        PreAnalysis result = preAnalyzer.preAnalyze(plan);
        assertThat(plan.preAnalyzed(), is(true));
        assertThat(result.indices, hasSize(1));
        assertThat(result.indices.get(0).id().cluster(), is("elastic"));
        assertThat(result.indices.get(0).id().index(), is("aaa"));
    }

    public void testComplicatedQuery() {
        LogicalPlan plan = parser.createStatement("SELECT MAX(a) FROM aaa WHERE d > 10 GROUP BY b HAVING AVG(c) ORDER BY e ASC");
        PreAnalysis result = preAnalyzer.preAnalyze(plan);
        assertThat(plan.preAnalyzed(), is(true));
        assertThat(result.indices, hasSize(1));
        assertThat(result.indices.get(0).id().cluster(), nullValue());
        assertThat(result.indices.get(0).id().index(), is("aaa"));
    }
}
