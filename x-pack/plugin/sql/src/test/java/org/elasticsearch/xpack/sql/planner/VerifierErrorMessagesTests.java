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
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.KeywordEsField;
import org.elasticsearch.xpack.sql.type.TextEsField;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

public class VerifierErrorMessagesTests extends ESTestCase {

    private SqlParser parser = new SqlParser();
    private Optimizer optimizer = new Optimizer();
    private Planner planner = new Planner();

    private String verify(String sql) {
        Map<String, EsField> mapping = new LinkedHashMap<>();
        mapping.put("bool", new EsField("bool", DataType.BOOLEAN, Collections.emptyMap(), true));
        mapping.put("int", new EsField("int", DataType.INTEGER, Collections.emptyMap(), true));
        mapping.put("text", new TextEsField("text", Collections.emptyMap(), true));
        mapping.put("keyword", new KeywordEsField("keyword", Collections.emptyMap(), true, DataType.KEYWORD.defaultPrecision, true));
        EsIndex test = new EsIndex("test", mapping);
        IndexResolution getIndexResult = IndexResolution.valid(test);
        Analyzer analyzer = new Analyzer(new FunctionRegistry(), getIndexResult, TimeZone.getTimeZone("UTC"));
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
