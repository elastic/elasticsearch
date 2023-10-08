/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.ql.index.IndexResolution;

import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.DISSECT;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.EVAL;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.GROK;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.LIMIT;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.SORT;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.STATS;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.WHERE;
import static org.elasticsearch.xpack.esql.stats.Metrics.FPREFIX;

public class VerifierMetricsTests extends ESTestCase {

    private EsqlParser parser = new EsqlParser();

    public void testDissectQuery() {
        Counters c = esql("from employees | dissect concat(first_name, \" \", last_name) \"%{a} %{b}\"");
        assertEquals(1L, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(1L, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(0, where(c));
    }

    public void testEvalQuery() {
        Counters c = esql("from employees | eval name_len = length(first_name)");
        assertEquals(0, dissect(c));
        assertEquals(1L, eval(c));
        assertEquals(0, grok(c));
        assertEquals(1L, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(0, where(c));
    }

    public void testGrokQuery() {
        Counters c = esql("from employees | grok concat(first_name, \" \", last_name) \"%{WORD:a} %{WORD:b}\"");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(1L, grok(c));
        assertEquals(1L, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(0, where(c));
    }

    public void testLimitQuery() {
        Counters c = esql("from employees | limit 2");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(1L, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(0, where(c));
    }

    public void testSortQuery() {
        Counters c = esql("from employees | sort first_name desc nulls first");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(1L, limit(c));
        assertEquals(1L, sort(c));
        assertEquals(0, stats(c));
        assertEquals(0, where(c));
    }

    public void testStatsQuery() {
        Counters c = esql("from employees | stats l = max(languages)");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(1L, limit(c));
        assertEquals(0, sort(c));
        assertEquals(1L, stats(c));
        assertEquals(0, where(c));
    }

    public void testWhereQuery() {
        Counters c = esql("from employees | where languages > 2");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(1L, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(1L, where(c));
    }

    public void testTwoWhereQuery() {
        Counters c = esql("from employees | where languages > 2 | limit 5 | sort first_name | where first_name == \"George\"");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(1L, limit(c));
        assertEquals(1L, sort(c));
        assertEquals(0, stats(c));
        assertEquals(1L, where(c));
    }

    public void testTwoQueriesExecuted() {
        Metrics metrics = new Metrics();
        Verifier verifier = new Verifier(metrics);
        esqlWithVerifier("""
               from employees
               | where languages > 2
               | limit 5
               | eval name_len = length(first_name)
               | sort first_name
               | limit 3
            """, verifier);
        esqlWithVerifier("""
              from employees
              | where languages > 2
              | sort first_name desc nulls first
              | dissect concat(first_name, " ", last_name) "%{a} %{b}"
              | grok concat(first_name, " ", last_name) "%{WORD:a} %{WORD:b}"
              | stats x = max(languages)
              | sort x
              | stats y = min(x) by x
            """, verifier);
        Counters c = metrics.stats();
        assertEquals(1L, dissect(c));
        assertEquals(1L, eval(c));
        assertEquals(1L, grok(c));
        assertEquals(2L, limit(c));
        assertEquals(2L, sort(c));
        assertEquals(1L, stats(c));
        assertEquals(2L, where(c));
    }

    private long dissect(Counters c) {
        return c.get(FPREFIX + DISSECT);
    }

    private long eval(Counters c) {
        return c.get(FPREFIX + EVAL);
    }

    private long grok(Counters c) {
        return c.get(FPREFIX + GROK);
    }

    private long limit(Counters c) {
        return c.get(FPREFIX + LIMIT);
    }

    private long sort(Counters c) {
        return c.get(FPREFIX + SORT);
    }

    private long stats(Counters c) {
        return c.get(FPREFIX + STATS);
    }

    private long where(Counters c) {
        return c.get(FPREFIX + WHERE);
    }

    private Counters esql(String sql) {
        return esql(sql, null);
    }

    private void esqlWithVerifier(String esql, Verifier verifier) {
        esql(esql, verifier);
    }

    private Counters esql(String esql, Verifier v) {
        IndexResolution mapping = AnalyzerTestUtils.analyzerDefaultMapping();

        Verifier verifier = v;
        Metrics metrics = null;
        if (v == null) {
            metrics = new Metrics();
            verifier = new Verifier(metrics);
        }
        analyzer(mapping, verifier).analyze(parser.createStatement(esql));

        return metrics == null ? null : metrics.stats();
    }
}
