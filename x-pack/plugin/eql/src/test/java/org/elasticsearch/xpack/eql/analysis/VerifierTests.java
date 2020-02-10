/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.TypesTests;

import java.util.Map;

public class VerifierTests extends ESTestCase {

    private EqlParser parser = new EqlParser();
    private IndexResolution index = IndexResolution.valid(new EsIndex("test", loadEqlMapping("mapping-default.json")));

    private LogicalPlan accept(IndexResolution resolution, String eql) {
        PreAnalyzer preAnalyzer = new PreAnalyzer();
        Analyzer analyzer = new Analyzer(new EqlFunctionRegistry(), new Verifier());
        return analyzer.analyze(preAnalyzer.preAnalyze(parser.createStatement(eql), resolution));
    }

    private LogicalPlan accept(String eql) {
        return accept(index, eql);
    }

    private String error(String sql) {
        return error(index, sql);
    }

    private String error(IndexResolution resolution, String eql) {
        VerificationException e = expectThrows(VerificationException.class, () -> accept(resolution, eql));
        assertTrue(e.getMessage().startsWith("Found "));
        String header = "Found 1 problem\nline ";
        return e.getMessage().substring(header.length());
    }

    public void testBasicQuery() {
        accept("foo where true");
    }

    public void testMissingColumn() {
        assertEquals("1:11: Unknown column [xxx]", error("foo where xxx == 100"));
    }
    
    public void testMisspelledColumn() {
        assertEquals("1:11: Unknown column [md4], did you mean [md5]?", error("foo where md4 == 1"));
    }

    public void testMisspelledColumnWithMultipleOptions() {
        assertEquals("1:11: Unknown column [pib], did you mean any of [pid, ppid]?", error("foo where pib == 1"));
    }


    private static Map<String, EsField> loadEqlMapping(String name) {
        return TypesTests.loadMapping(name);
    }
}
