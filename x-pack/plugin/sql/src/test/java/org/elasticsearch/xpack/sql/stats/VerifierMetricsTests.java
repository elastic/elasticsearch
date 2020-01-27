/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.stats;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;

import java.util.Map;

import static org.elasticsearch.xpack.sql.stats.FeatureMetric.COMMAND;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.GROUPBY;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.HAVING;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.LIMIT;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.LOCAL;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.ORDERBY;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.WHERE;
import static org.elasticsearch.xpack.sql.stats.Metrics.FPREFIX;

public class VerifierMetricsTests extends ESTestCase {
    
    private SqlParser parser = new SqlParser();
    private String[] commands = {"SHOW FUNCTIONS", "SHOW COLUMNS FROM library", "SHOW SCHEMAS",
                                 "SHOW TABLES", "SYS COLUMNS LIKE '%name'", "SYS TABLES", "SYS TYPES"};
    
    public void testWhereQuery() {
        Counters c = sql("SELECT emp_no FROM test WHERE languages > 2");
        assertEquals(1L, where(c));
        assertEquals(0, limit(c));
        assertEquals(0, groupby(c));
        assertEquals(0, having(c));
        assertEquals(0, orderby(c));
        assertEquals(0, command(c));
        assertEquals(0, local(c));
    }
    
    public void testLimitQuery() {
        Counters c = sql("SELECT emp_no FROM test LIMIT 4");
        assertEquals(0, where(c));
        assertEquals(1L, limit(c));
        assertEquals(0, groupby(c));
        assertEquals(0, having(c));
        assertEquals(0, orderby(c));
        assertEquals(0, command(c));
        assertEquals(0, local(c));
    }
    
    public void testGroupByQuery() {
        Counters c = sql("SELECT languages, MAX(languages) FROM test GROUP BY languages");
        assertEquals(0, where(c));
        assertEquals(0, limit(c));
        assertEquals(1L, groupby(c));
        assertEquals(0, having(c));
        assertEquals(0, orderby(c));
        assertEquals(0, command(c));
        assertEquals(0, local(c));
    }
    
    public void testHavingQuery() {
        Counters c = sql("SELECT UCASE(gender), MAX(languages) FROM test GROUP BY gender HAVING MAX(languages) > 3");
        assertEquals(0, where(c));
        assertEquals(0, limit(c));
        assertEquals(1L, groupby(c));
        assertEquals(1L, having(c));
        assertEquals(0, orderby(c));
        assertEquals(0, command(c));
        assertEquals(0, local(c));
    }
    
    public void testOrderByQuery() {
        Counters c = sql("SELECT UCASE(gender) FROM test ORDER BY emp_no");
        assertEquals(0, where(c));
        assertEquals(0, limit(c));
        assertEquals(0, groupby(c));
        assertEquals(0, having(c));
        assertEquals(1L, orderby(c));
        assertEquals(0, command(c));
        assertEquals(0, local(c));
    }
    
    public void testCommand() {
        Counters c = sql(randomFrom("SHOW FUNCTIONS", "SHOW COLUMNS FROM library", "SHOW SCHEMAS",
                                    "SHOW TABLES", "SYS COLUMNS LIKE '%name'", "SYS TABLES", "SYS TYPES"));
        assertEquals(0, where(c));
        assertEquals(0, limit(c));
        assertEquals(0, groupby(c));
        assertEquals(0, having(c));
        assertEquals(0, orderby(c));
        assertEquals(1L, command(c));
        assertEquals(0, local(c));
    }
    
    public void testLocalQuery() {
        Counters c = sql("SELECT CONCAT('Elastic','search')");
        assertEquals(0, where(c));
        assertEquals(0, limit(c));
        assertEquals(0, groupby(c));
        assertEquals(0, having(c));
        assertEquals(0, orderby(c));
        assertEquals(0, command(c));
        assertEquals(1L, local(c));
    }
    
    public void testWhereAndLimitQuery() {
        Counters c = sql("SELECT emp_no FROM test WHERE languages > 2 LIMIT 5");
        assertEquals(1L, where(c));
        assertEquals(1L, limit(c));
        assertEquals(0, groupby(c));
        assertEquals(0, having(c));
        assertEquals(0, orderby(c));
        assertEquals(0, command(c));
        assertEquals(0, local(c));
    }
    
    public void testWhereLimitGroupByQuery() {
        Counters c = sql("SELECT languages FROM test WHERE languages > 2 GROUP BY languages LIMIT 5");
        assertEquals(1L, where(c));
        assertEquals(1L, limit(c));
        assertEquals(1L, groupby(c));
        assertEquals(0, having(c));
        assertEquals(0, orderby(c));
        assertEquals(0, command(c));
        assertEquals(0, local(c));
    }
    
    public void testWhereLimitGroupByHavingQuery() {
        Counters c = sql("SELECT languages FROM test WHERE languages > 2 GROUP BY languages HAVING MAX(languages) > 3 LIMIT 5");
        assertEquals(1L, where(c));
        assertEquals(1L, limit(c));
        assertEquals(1L, groupby(c));
        assertEquals(1L, having(c));
        assertEquals(0, orderby(c));
        assertEquals(0, command(c));
        assertEquals(0, local(c));
    }
    
    public void testWhereLimitGroupByHavingOrderByQuery() {
        Counters c = sql("SELECT languages FROM test WHERE languages > 2 GROUP BY languages HAVING MAX(languages) > 3"
                      + " ORDER BY languages LIMIT 5");
        assertEquals(1L, where(c));
        assertEquals(1L, limit(c));
        assertEquals(1L, groupby(c));
        assertEquals(1L, having(c));
        assertEquals(1L, orderby(c));
        assertEquals(0, command(c));
        assertEquals(0, local(c));
    }
    
    public void testTwoQueriesExecuted() {
        Metrics metrics = new Metrics();
        Verifier verifier = new Verifier(metrics);
        sqlWithVerifier("SELECT languages FROM test WHERE languages > 2 GROUP BY languages LIMIT 5", verifier);
        sqlWithVerifier("SELECT languages FROM test WHERE languages > 2 GROUP BY languages HAVING MAX(languages) > 3 "
                      + "ORDER BY languages LIMIT 5", verifier);
        Counters c = metrics.stats();
        
        assertEquals(2L, where(c));
        assertEquals(2L, limit(c));
        assertEquals(2L, groupby(c));
        assertEquals(1L, having(c));
        assertEquals(1L, orderby(c));
        assertEquals(0, command(c));
        assertEquals(0, local(c));
    }
    
    public void testTwoCommandsExecuted() {
        String command1 = randomFrom(commands);
        Metrics metrics = new Metrics();
        Verifier verifier = new Verifier(metrics);
        sqlWithVerifier(command1, verifier);
        sqlWithVerifier(randomValueOtherThan(command1, () -> randomFrom(commands)), verifier);
        Counters c = metrics.stats();
        
        assertEquals(0, where(c));
        assertEquals(0, limit(c));
        assertEquals(0, groupby(c));
        assertEquals(0, having(c));
        assertEquals(0, orderby(c));
        assertEquals(2, command(c));
        assertEquals(0, local(c));
    }
    
    private long where(Counters c) {
        return c.get(FPREFIX + WHERE);
    }
    
    private long groupby(Counters c) {
        return c.get(FPREFIX + GROUPBY);
    }
    
    private long limit(Counters c) {
        return c.get(FPREFIX + LIMIT);
    }
    
    private long local(Counters c) {
        return c.get(FPREFIX + LOCAL);
    }
    
    private long having(Counters c) {
        return c.get(FPREFIX + HAVING);
    }
    
    private long orderby(Counters c) {
        return c.get(FPREFIX + ORDERBY);
    }
    
    private long command(Counters c) {
        return c.get(FPREFIX + COMMAND);
    }
    
    private Counters sql(String sql) {
        return sql(sql, null);
    }
    
    private void sqlWithVerifier(String sql, Verifier verifier) {
        sql(sql, verifier);
    }

    private Counters sql(String sql, Verifier v) {
        Map<String, EsField> mapping = SqlTypesTests.loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping);
        
        Verifier verifier = v;
        Metrics metrics = null;
        if (v == null) {
            metrics = new Metrics();
            verifier = new Verifier(metrics);
        }

        Analyzer analyzer = new Analyzer(SqlTestUtils.TEST_CFG, new SqlFunctionRegistry(), IndexResolution.valid(test), verifier);
        analyzer.analyze(parser.createStatement(sql), true);
        
        return metrics == null ? null : metrics.stats();
    }
}