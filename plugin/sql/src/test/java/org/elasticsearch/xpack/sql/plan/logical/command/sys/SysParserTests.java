/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.TypesTests;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class SysParserTests extends ESTestCase {

    private final SqlParser parser = new SqlParser(DateTimeZone.UTC);

    private Command sql(String sql) {
        Map<String, EsField> mapping = TypesTests.loadMapping("mapping-multi-field-with-nested.json");
        EsIndex test = new EsIndex("test", mapping);
        Analyzer analyzer = new Analyzer(new FunctionRegistry(), IndexResolution.valid(test), DateTimeZone.UTC);
        return (Command) analyzer.analyze(parser.createStatement(sql), true);
    }

    public void testSysTypes() throws Exception {
        Command cmd = sql("SYS TYPES");

        List<String> names = asList("BYTE", "SHORT", "INTEGER", "LONG", "HALF_FLOAT", "SCALED_FLOAT", "FLOAT", "DOUBLE", "KEYWORD", "TEXT",
                "DATE", "BINARY", "NULL", "UNSUPPORTED", "OBJECT", "NESTED", "BOOLEAN");

        cmd.execute(null, ActionListener.wrap(r -> {
            assertEquals(19, r.columnCount());
            assertEquals(17, r.size());

            for (int i = 0; i < r.size(); i++) {
                assertEquals(names.get(i), r.column(0));
                r.advanceRow();
            }

        }, ex -> fail(ex.getMessage())));
    }
}
