/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.AnalysisException;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.TypesTests;

import java.util.Map;
import java.util.TimeZone;

public class VerifierErrorMessagesTests extends ESTestCase {
    private SqlParser parser = new SqlParser();

    private String verify(String sql) {
        Map<String, EsField> mapping = TypesTests.loadMapping("mapping-multi-field-with-nested.json");
        EsIndex test = new EsIndex("test", mapping);
        return verify(IndexResolution.valid(test), sql);
    }

    private String verify(IndexResolution getIndexResult, String sql) {
        Analyzer analyzer = new Analyzer(new FunctionRegistry(), getIndexResult, TimeZone.getTimeZone("UTC"));
        AnalysisException e = expectThrows(AnalysisException.class, () -> analyzer.analyze(parser.createStatement(sql), true));
        assertTrue(e.getMessage().startsWith("Found "));
        String header = "Found 1 problem(s)\nline ";
        return e.getMessage().substring(header.length());
    }

    public void testMissingIndex() {
        assertEquals("1:17: Unknown index [missing]", verify(IndexResolution.notFound("missing"), "SELECT foo FROM missing"));
    }

    public void testMissingColumn() {
        assertEquals("1:8: Unknown column [xxx]", verify("SELECT xxx FROM test"));
    }

    public void testMisspelledColumn() {
        assertEquals("1:8: Unknown column [txt], did you mean [text]?", verify("SELECT txt FROM test"));
    }

    public void testFunctionOverMissingField() {
        assertEquals("1:12: Unknown column [xxx]", verify("SELECT ABS(xxx) FROM test"));
    }

    public void testMissingFunction() {
        assertEquals("1:8: Unknown function [ZAZ]", verify("SELECT ZAZ(bool) FROM test"));
    }

    public void testMisspelledFunction() {
        assertEquals("1:8: Unknown function [COONT], did you mean any of [COUNT, COT]?", verify("SELECT COONT(bool) FROM test"));
    }

    public void testMissingColumnInGroupBy() {
        assertEquals("1:41: Unknown column [xxx]", verify("SELECT * FROM test GROUP BY DAY_OF_YEAR(xxx)"));
    }

    public void testFilterOnUnknownColumn() {
        assertEquals("1:26: Unknown column [xxx]", verify("SELECT * FROM test WHERE xxx = 1"));
    }

    public void testMissingColumnInOrderBy() {
        // xxx offset is that of the order by field
        assertEquals("1:29: Unknown column [xxx]", verify("SELECT * FROM test ORDER BY xxx"));
    }

    public void testMissingColumnFunctionInOrderBy() {
        // xxx offset is that of the order by field
        assertEquals("1:41: Unknown column [xxx]", verify("SELECT * FROM test ORDER BY DAY_oF_YEAR(xxx)"));
    }

    public void testMissingExtract() {
        assertEquals("1:8: Unknown datetime field [ZAZ]", verify("SELECT EXTRACT(ZAZ FROM date) FROM test"));
    }

    public void testMissingExtractSimilar() {
        assertEquals("1:8: Unknown datetime field [DAP], did you mean [DAY]?", verify("SELECT EXTRACT(DAP FROM date) FROM test"));
    }

    public void testMissingExtractSimilarMany() {
        assertEquals("1:8: Unknown datetime field [DOP], did you mean any of [DOM, DOW, DOY]?",
            verify("SELECT EXTRACT(DOP FROM date) FROM test"));
    }

    public void testExtractNonDateTime() {
        assertEquals("1:8: Invalid datetime field [ABS]. Use any datetime function.", verify("SELECT EXTRACT(ABS FROM date) FROM test"));
    }

    public void testMultipleColumns() {
        // xxx offset is that of the order by field
        assertEquals("1:43: Unknown column [xxx]\nline 1:8: Unknown column [xxx]",
                verify("SELECT xxx FROM test GROUP BY DAY_oF_YEAR(xxx)"));
    }

    // GROUP BY
    public void testGroupBySelectNonGrouped() {
        assertEquals("1:8: Cannot use non-grouped column [text], expected [int]",
                verify("SELECT text, int FROM test GROUP BY int"));
    }

    public void testGroupByOrderByNonGrouped() {
        assertEquals("1:50: Cannot order by non-grouped column [bool], expected [text]",
                verify("SELECT MAX(int) FROM test GROUP BY text ORDER BY bool"));
    }

    public void testGroupByOrderByScalarOverNonGrouped() {
        assertEquals("1:50: Cannot order by non-grouped column [YEAR(date [UTC])], expected [text]",
                verify("SELECT MAX(int) FROM test GROUP BY text ORDER BY YEAR(date)"));
    }

    public void testGroupByHavingNonGrouped() {
        assertEquals("1:48: Cannot filter by non-grouped column [int], expected [text]",
                verify("SELECT AVG(int) FROM test GROUP BY text HAVING int > 10"));
    }

    public void testGroupByAggregate() {
        assertEquals("1:36: Cannot use an aggregate [AVG] for grouping",
                verify("SELECT AVG(int) FROM test GROUP BY AVG(int)"));
    }

    public void testGroupByOnNested() {
        assertEquals("1:38: Grouping isn't (yet) compatible with nested fields [dep.dep_id]",
                verify("SELECT dep.dep_id FROM test GROUP BY dep.dep_id"));
    }

    public void testHavingOnNested() {
        assertEquals("1:51: HAVING isn't (yet) compatible with nested fields [dep.start_date]",
                verify("SELECT int FROM test GROUP BY int HAVING AVG(YEAR(dep.start_date)) > 1980"));
    }

    public void testGroupByScalarFunctionWithAggOnTarget() {
        assertEquals("1:31: Cannot use an aggregate [AVG] for grouping",
                verify("SELECT int FROM test GROUP BY AVG(int) + 2"));
    }

    public void testUnsupportedType() {
        assertEquals("1:8: Cannot use field [unsupported] type [ip_range] as is unsupported",
                verify("SELECT unsupported FROM test"));
    }

    public void testGroupByOrderByNonKey() {
        assertEquals("1:52: Cannot order by non-grouped column [a], expected [bool]",
                verify("SELECT AVG(int) a FROM test GROUP BY bool ORDER BY a"));
    }

    public void testGroupByOrderByFunctionOverKey() {
        assertEquals("1:44: Cannot order by non-grouped column [MAX(int)], expected [int]",
                verify("SELECT int FROM test GROUP BY int ORDER BY MAX(int)"));
    }

    public void testGroupByOrderByScore() {
        assertEquals("1:44: Cannot order by non-grouped column [SCORE()], expected [int]",
                verify("SELECT int FROM test GROUP BY int ORDER BY SCORE()"));
    }

    public void testHavingOnColumn() {
        assertEquals("1:42: Cannot filter HAVING on non-aggregate [int]; consider using WHERE instead",
                verify("SELECT int FROM test GROUP BY int HAVING int > 2"));
    }

    public void testHavingOnScalar() {
        assertEquals("1:42: Cannot filter HAVING on non-aggregate [int]; consider using WHERE instead",
                verify("SELECT int FROM test GROUP BY int HAVING 2 < ABS(int)"));
    }
}