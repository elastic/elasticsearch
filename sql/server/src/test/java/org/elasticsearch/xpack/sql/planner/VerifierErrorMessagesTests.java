/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.analysis.catalog.EsIndex;
import org.elasticsearch.xpack.sql.analysis.catalog.InMemoryCatalog;
import org.elasticsearch.xpack.sql.expression.function.DefaultFunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.session.TestingSqlSession;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.junit.After;
import org.junit.Before;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.singletonList;

public class VerifierErrorMessagesTests extends ESTestCase {

    private SqlParser parser;
    private FunctionRegistry functionRegistry;
    private Catalog catalog;
    private Analyzer analyzer;
    private Optimizer optimizer;
    private Planner planner;

    public VerifierErrorMessagesTests() {
        parser = new SqlParser();
        functionRegistry = new DefaultFunctionRegistry();

        Map<String, DataType> mapping = new LinkedHashMap<>();
        mapping.put("bool", DataTypes.BOOLEAN);
        mapping.put("int", DataTypes.INTEGER);
        mapping.put("text", DataTypes.TEXT);
        mapping.put("keyword", DataTypes.KEYWORD);
        EsIndex test = new EsIndex("test", mapping);
        catalog = new InMemoryCatalog(singletonList(test));
        analyzer = new Analyzer(functionRegistry);
        optimizer = new Optimizer();
        planner = new Planner();

    }

    @Before
    public void setupContext() {
        TestingSqlSession.setCurrentContext(TestingSqlSession.ctx(catalog));
    }

    @After
    public void disposeContext() {
        TestingSqlSession.removeCurrentContext();
    }

    private String verify(String sql) {
        PlanningException e = expectThrows(PlanningException.class,
                () -> planner.mapPlan(optimizer.optimize(analyzer.analyze(parser.createStatement(sql), true)), true));
        assertTrue(e.getMessage().startsWith("Found "));
        String header = "Found 1 problem(s)\nline ";
        return e.getMessage().substring(header.length());
    }


    public void testMultiGroupBy() {
        // TODO: location needs to be updated after merging extend-having
        assertEquals("1:32: Currently, only a single expression can be used with GROUP BY; please select one of [bool, keyword]",
                verify("SELECT bool FROM test GROUP BY bool, keyword"));
    }
}