/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.AnalysisException;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.analysis.catalog.EsIndex;
import org.elasticsearch.xpack.sql.analysis.catalog.InMemoryCatalog;
import org.elasticsearch.xpack.sql.expression.function.DefaultFunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class VerifierErrorMessagesTests extends ESTestCase {

    private SqlParser parser;
    private FunctionRegistry functionRegistry;
    private Catalog catalog;
    private Analyzer analyzer;

    public VerifierErrorMessagesTests() {
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
        AnalysisException e = expectThrows(AnalysisException.class, () -> analyzer.analyze(parser.createStatement(sql), true));
        assertTrue(e.getMessage().startsWith("Found "));
        String header = "Found 1 problem(s)\nline ";
        return e.getMessage().substring(header.length());
    }

    public void testMissingIndex() {
        assertEquals("1:17: Unknown index [missing]", verify("SELECT foo FROM missing"));
    }

    public void testMissingColumn() {
        assertEquals("1:8: Unknown column [xxx]", verify("SELECT xxx FROM test"));
    }
    
    public void testMispelledColumn() {
        assertEquals("1:8: Unknown column [txt], did you mean [text]?", verify("SELECT txt FROM test"));
    }

    public void testFunctionOverMissingField() {
        assertEquals("1:12: Unknown column [xxx]", verify("SELECT ABS(xxx) FROM test"));
    }

    public void testMissingFunction() {
        assertEquals("1:8: Unknown function [ZAZ]", verify("SELECT ZAZ(bool) FROM test"));
    }

    public void testMisspelledFunction() {
        assertEquals("1:8: Unknown function [COONT], did you mean [COUNT]?", verify("SELECT COONT(bool) FROM test"));
    }

    public void testMissingColumnInGroupBy() {
        assertEquals("1:41: Unknown column [xxx]", verify("SELECT * FROM test GROUP BY DAY_OF_YEAR(xxx)"));
    }

    public void testFilterOnUnknownColumn() {
        assertEquals("1:26: Unknown column [xxx]", verify("SELECT * FROM test WHERE xxx = 1"));
    }

    public void testMissingColumnInOrderby() {
        // xxx offset is that of the order by field
        assertEquals("1:29: Unknown column [xxx]", verify("SELECT * FROM test ORDER BY xxx"));
    }

    public void testMissingColumnFunctionInOrderby() {
        // xxx offset is that of the order by field
        assertEquals("1:41: Unknown column [xxx]", verify("SELECT * FROM test ORDER BY DAY_oF_YEAR(xxx)"));
    }

    public void testMultipleColumns() {
        // xxx offset is that of the order by field
        assertEquals("1:43: Unknown column [xxx]\nline 1:8: Unknown column [xxx]",
                verify("SELECT xxx FROM test GROUP BY DAY_oF_YEAR(xxx)"));
    }
}