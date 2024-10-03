/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.parser.EsqlParser;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.DISSECT;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.DROP;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.ENRICH;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.EVAL;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.FROM;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.GROK;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.KEEP;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.LIMIT;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.MV_EXPAND;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.RENAME;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.ROW;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.SHOW;
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
        assertEquals(0, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(0, where(c));
        assertEquals(0, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(0, drop(c));
        assertEquals(0, keep(c));
        assertEquals(0, rename(c));
    }

    public void testEvalQuery() {
        Counters c = esql("from employees | eval name_len = length(first_name)");
        assertEquals(0, dissect(c));
        assertEquals(1L, eval(c));
        assertEquals(0, grok(c));
        assertEquals(0, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(0, where(c));
        assertEquals(0, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(0, drop(c));
        assertEquals(0, keep(c));
        assertEquals(0, rename(c));
    }

    public void testGrokQuery() {
        Counters c = esql("from employees | grok concat(first_name, \" \", last_name) \"%{WORD:a} %{WORD:b}\"");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(1L, grok(c));
        assertEquals(0, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(0, where(c));
        assertEquals(0, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(0, drop(c));
        assertEquals(0, keep(c));
        assertEquals(0, rename(c));
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
        assertEquals(0, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(0, drop(c));
        assertEquals(0, keep(c));
        assertEquals(0, rename(c));
    }

    public void testSortQuery() {
        Counters c = esql("from employees | sort first_name desc nulls first");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(0, limit(c));
        assertEquals(1L, sort(c));
        assertEquals(0, stats(c));
        assertEquals(0, where(c));
        assertEquals(0, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(0, drop(c));
        assertEquals(0, keep(c));
        assertEquals(0, rename(c));
    }

    public void testStatsQuery() {
        Counters c = esql("from employees | stats l = max(languages)");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(0, limit(c));
        assertEquals(0, sort(c));
        assertEquals(1L, stats(c));
        assertEquals(0, where(c));
        assertEquals(0, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(0, drop(c));
        assertEquals(0, keep(c));
        assertEquals(0, rename(c));
    }

    public void testWhereQuery() {
        Counters c = esql("from employees | where languages > 2");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(0, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(1L, where(c));
        assertEquals(0, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(0, drop(c));
        assertEquals(0, keep(c));
        assertEquals(0, rename(c));
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
        assertEquals(0, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(0, drop(c));
        assertEquals(0, keep(c));
        assertEquals(0, rename(c));
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
        assertEquals(1L, limit(c));
        assertEquals(2L, sort(c));
        assertEquals(1L, stats(c));
        assertEquals(2L, where(c));
        assertEquals(0, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(2L, from(c));
        assertEquals(0, drop(c));
        assertEquals(0, keep(c));
        assertEquals(0, rename(c));
    }

    public void testEnrich() {
        Counters c = esql("""
            from employees
            | sort emp_no
            | limit 1
            | eval x = to_string(languages)
            | enrich languages on x
            | keep emp_no, language_name""");
        assertEquals(0, dissect(c));
        assertEquals(1L, eval(c));
        assertEquals(0, grok(c));
        assertEquals(1L, limit(c));
        assertEquals(1L, sort(c));
        assertEquals(0, stats(c));
        assertEquals(0, where(c));
        assertEquals(1L, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(0, drop(c));
        assertEquals(1L, keep(c));
        assertEquals(0, rename(c));
    }

    public void testMvExpand() {
        Counters c = esql("""
            from employees
            | where emp_no == 10004
            | limit 1
            | keep emp_no, job
            | mv_expand job
            | where job LIKE \"*a*\"
            | limit 2
            | where job LIKE \"*a*\"
            | limit 3""");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(1L, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(1L, where(c));
        assertEquals(0, enrich(c));
        assertEquals(1L, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(0, drop(c));
        assertEquals(1L, keep(c));
        assertEquals(0, rename(c));
    }

    public void testShowInfo() {
        Counters c = esql("show info |  stats  a = count(*), b = count(*), c = count(*) |  mv_expand c");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(0, limit(c));
        assertEquals(0, sort(c));
        assertEquals(1L, stats(c));
        assertEquals(0, where(c));
        assertEquals(0, enrich(c));
        assertEquals(1L, mvExpand(c));
        assertEquals(1L, show(c));
        assertEquals(0, row(c));
        assertEquals(0, from(c));
        assertEquals(0, drop(c));
        assertEquals(0, keep(c));
        assertEquals(0, rename(c));
    }

    public void testRow() {
        Counters c = esql("row a = [\"1\", \"2\"] | enrich languages on a with a_lang = language_name");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(0, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(0, where(c));
        assertEquals(1L, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(1L, row(c));
        assertEquals(0, from(c));
        assertEquals(0, drop(c));
        assertEquals(0, keep(c));
        assertEquals(0, rename(c));
    }

    public void testDropAndRename() {
        Counters c = esql("from employees | rename gender AS foo | stats bar = count(*) by foo | drop foo | sort bar | drop bar");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(0, limit(c));
        assertEquals(1L, sort(c));
        assertEquals(1L, stats(c));
        assertEquals(0, where(c));
        assertEquals(0, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(1L, drop(c));
        assertEquals(0, keep(c));
        assertEquals(1L, rename(c));
    }

    public void testKeep() {
        Counters c = esql("""
            from employees
            | keep emp_no, languages
            | where languages is null or emp_no <= 10030
            | where languages in (2, 3, emp_no)
            | keep languages""");
        assertEquals(0, dissect(c));
        assertEquals(0, eval(c));
        assertEquals(0, grok(c));
        assertEquals(0, limit(c));
        assertEquals(0, sort(c));
        assertEquals(0, stats(c));
        assertEquals(1L, where(c));
        assertEquals(0, enrich(c));
        assertEquals(0, mvExpand(c));
        assertEquals(0, show(c));
        assertEquals(0, row(c));
        assertEquals(1L, from(c));
        assertEquals(0, drop(c));
        assertEquals(1L, keep(c));
        assertEquals(0, rename(c));
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

    private long enrich(Counters c) {
        return c.get(FPREFIX + ENRICH);
    }

    private long mvExpand(Counters c) {
        return c.get(FPREFIX + MV_EXPAND);
    }

    private long show(Counters c) {
        return c.get(FPREFIX + SHOW);
    }

    private long row(Counters c) {
        return c.get(FPREFIX + ROW);
    }

    private long from(Counters c) {
        return c.get(FPREFIX + FROM);
    }

    private long drop(Counters c) {
        return c.get(FPREFIX + DROP);
    }

    private long keep(Counters c) {
        return c.get(FPREFIX + KEEP);
    }

    private long rename(Counters c) {
        return c.get(FPREFIX + RENAME);
    }

    private Counters esql(String esql) {
        return esql(esql, null);
    }

    private void esqlWithVerifier(String esql, Verifier verifier) {
        esql(esql, verifier);
    }

    private Counters esql(String esql, Verifier v) {
        Verifier verifier = v;
        Metrics metrics = null;
        if (v == null) {
            metrics = new Metrics();
            verifier = new Verifier(metrics);
        }
        analyzer(verifier).analyze(parser.createStatement(esql));

        return metrics == null ? null : metrics.stats();
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
