/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.LoadMapping;
import org.elasticsearch.xpack.esql.core.ParsingException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ParsingTests extends ESTestCase {
    private static final String INDEX_NAME = "test";
    private static final EsqlParser parser = new EsqlParser();

    private final IndexResolution defaultIndex = loadIndexResolution("mapping-basic.json");
    private final Analyzer defaultAnalyzer = new Analyzer(
        new AnalyzerContext(TEST_CFG, new EsqlFunctionRegistry(), defaultIndex, emptyPolicyResolution()),
        TEST_VERIFIER
    );

    public void testCaseFunctionInvalidInputs() {
        assertEquals("1:23: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case()"));
        assertEquals("1:23: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case(a)"));
        assertEquals("1:23: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case(1)"));
    }

    public void testConcatFunctionInvalidInputs() {
        assertEquals("1:23: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat()"));
        assertEquals("1:23: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat(a)"));
        assertEquals("1:23: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat(1)"));
    }

    public void testCoalesceFunctionInvalidInputs() {
        assertEquals("1:23: error building [coalesce]: expects at least one argument", error("row a = 1 | eval x = coalesce()"));
    }

    public void testGreatestFunctionInvalidInputs() {
        assertEquals("1:23: error building [greatest]: expects at least one argument", error("row a = 1 | eval x = greatest()"));
    }

    public void testLeastFunctionInvalidInputs() {
        assertEquals("1:23: error building [least]: expects at least one argument", error("row a = 1 | eval x = least()"));
    }

    /**
     * Tests the inline cast syntax {@code <value>::<type>} for all supported types and
     * builds a little json report of the valid types.
     */
    public void testInlineCast() throws IOException {
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry();
        Path dir = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("esql").resolve("functions").resolve("kibana");
        Files.createDirectories(dir);
        Path file = dir.resolve("inline_cast.json");
        try (XContentBuilder report = new XContentBuilder(JsonXContent.jsonXContent, Files.newOutputStream(file))) {
            report.humanReadable(true).prettyPrint();
            report.startObject();
            List<String> namesAndAliases = new ArrayList<>(DataType.namesAndAliases());
            Collections.sort(namesAndAliases);
            for (String nameOrAlias : namesAndAliases) {
                DataType expectedType = DataType.fromNameOrAlias(nameOrAlias);
                if (expectedType == DataType.TEXT) {
                    expectedType = DataType.KEYWORD;
                }
                if (EsqlDataTypeConverter.converterFunctionFactory(expectedType) == null) {
                    continue;
                }
                LogicalPlan plan = parser.createStatement("ROW a = 1::" + nameOrAlias);
                Row row = as(plan, Row.class);
                assertThat(row.fields(), hasSize(1));
                Expression functionCall = row.fields().get(0).child();
                assertThat(functionCall.dataType(), equalTo(expectedType));
                report.field(nameOrAlias, functionName(registry, functionCall));
            }
            report.endObject();
        }
        logger.info("Wrote to file: {}", file);
    }

    private String functionName(EsqlFunctionRegistry registry, Expression functionCall) {
        for (FunctionDefinition def : registry.listFunctions()) {
            if (functionCall.getClass().equals(def.clazz())) {
                return def.name();
            }
        }
        throw new IllegalArgumentException("can't find name for " + functionCall);
    }

    private String error(String query) {
        ParsingException e = expectThrows(ParsingException.class, () -> defaultAnalyzer.analyze(parser.createStatement(query)));
        String message = e.getMessage();
        assertTrue(message.startsWith("line "));
        return message.substring("line ".length());
    }

    private static IndexResolution loadIndexResolution(String name) {
        return IndexResolution.valid(new EsIndex(INDEX_NAME, LoadMapping.loadMapping(name)));
    }
}
