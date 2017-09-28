/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.analysis.AnalysisException;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.analysis.catalog.EsIndex;
import org.elasticsearch.xpack.sql.analysis.catalog.InMemoryCatalog;
import org.elasticsearch.xpack.sql.expression.function.DefaultFunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class VerifierErrorMessagesTest {

    private SqlParser parser;
    private FunctionRegistry functionRegistry;
    private Catalog catalog;
    private Analyzer analyzer;

    public VerifierErrorMessagesTest() {
        parser = new SqlParser();
        functionRegistry = new DefaultFunctionRegistry();

        Map<String, DataType> mapping = new LinkedHashMap<>();
        mapping.put("bool", DataTypes.BOOLEAN);
        mapping.put("int", DataTypes.INTEGER);
        mapping.put("text", DataTypes.TEXT);
        mapping.put("keyword", DataTypes.KEYWORD);
        EsIndex test = new EsIndex("test", mapping, emptyList(), Settings.EMPTY);
        catalog = new InMemoryCatalog(singletonList(test));
        analyzer = new Analyzer(functionRegistry, catalog);
    }

    private String verify(String sql) {
        try {
            analyzer.analyze(parser.createStatement(sql), true);
            fail("query is valid; expected an invalid one");
            return "";
        } catch (AnalysisException ae) {
            String message = ae.getMessage();
            assertTrue(message.startsWith("Found "));
            // test uses 1 or multiple
            String header = "Found 1 problem(s)\nline ";
            return message.substring(header.length());
        }
    }

    @Test
    public void testMissingIndex() {
        assertEquals("1:17: Unknown index [missing]", verify("SELECT foo FROM missing"));
    }

    @Test
    public void testMissingColumn() {
        assertEquals("1:8: Unknown column [xxx]", verify("SELECT xxx FROM test"));
    }
    
    @Test
    public void testMispelledColumn() {
        assertEquals("1:8: Unknown column [txt], did you mean [text]?", verify("SELECT txt FROM test"));
    }

    @Test
    public void testFunctionOverMissingField() {
        assertEquals("1:12: Unknown column [xxx]", verify("SELECT ABS(xxx) FROM test"));
    }

    @Test
    public void testMissingFunction() {
        assertEquals("1:8: Unknown function [ZAZ]", verify("SELECT ZAZ(bool) FROM test"));
    }

    @Test
    public void testMispelledFunction() {
        assertEquals("1:8: Unknown function [COONT], did you mean [COUNT]?", verify("SELECT COONT(bool) FROM test"));
    }

    @Test
    public void testMissingColumnInGroupBy() {
        assertEquals("1:41: Unknown column [xxx]", verify("SELECT * FROM test GROUP BY DAY_OF_YEAR(xxx)"));
    }

    @Test
    public void testFilterOnUnknownColumn() {
        assertEquals("1:26: Unknown column [xxx]", verify("SELECT * FROM test WHERE xxx = 1"));
    }

    @Test
    public void testMissingColumnInOrderby() {
        // xxx offset is that of the order by field
        assertEquals("1:29: Unknown column [xxx]", verify("SELECT * FROM test ORDER BY xxx"));
    }

    @Test
    public void testMissingColumnFunctionInOrderby() {
        // xxx offset is that of the order by field
        assertEquals("1:41: Unknown column [xxx]", verify("SELECT * FROM test ORDER BY DAY_oF_YEAR(xxx)"));
    }

    @Test
    public void testMultipleColumns() {
        // xxx offset is that of the order by field
        assertEquals("1:43: Unknown column [xxx]\nline 1:8: Unknown column [xxx]", verify("SELECT xxx FROM test GROUP BY DAY_oF_YEAR(xxx)"));
    }
}