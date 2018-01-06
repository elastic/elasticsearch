/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.joda.time.DateTimeZone;

import java.util.LinkedHashMap;
import java.util.Map;

public class VerifierErrorMessagesTests extends ESTestCase {

    private SqlParser parser = new SqlParser(DateTimeZone.UTC);
    private Optimizer optimizer = new Optimizer();
    private Planner planner = new Planner();

    private String verify(String sql) {
        Map<String, DataType> mapping = new LinkedHashMap<>();
        mapping.put("bool", DataTypes.BOOLEAN);
        mapping.put("int", DataTypes.INTEGER);
        mapping.put("text", DataTypes.TEXT);
        mapping.put("keyword", DataTypes.KEYWORD);
        EsIndex test = new EsIndex("test", mapping);
        IndexResolution getIndexResult = IndexResolution.valid(test);
        Analyzer analyzer = new Analyzer(new FunctionRegistry(), getIndexResult, DateTimeZone.UTC);
        LogicalPlan plan = optimizer.optimize(analyzer.analyze(parser.createStatement(sql), true));
        PlanningException e = expectThrows(PlanningException.class, () -> planner.mapPlan(plan, true));
        assertTrue(e.getMessage().startsWith("Found "));
        String header = "Found 1 problem(s)\nline ";
        return e.getMessage().substring(header.length());
    }

    public void testMultiGroupBy() {
        assertEquals("1:32: Currently, only a single expression can be used with GROUP BY; please select one of [bool, keyword]",
                verify("SELECT bool FROM test GROUP BY bool, keyword"));
    }
}
