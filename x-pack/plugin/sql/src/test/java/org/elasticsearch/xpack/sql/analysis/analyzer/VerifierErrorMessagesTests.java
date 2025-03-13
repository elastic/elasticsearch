/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolverTests;
import org.elasticsearch.xpack.sql.expression.function.aggregate.First;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Truncate;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Char;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Space;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Coalesce;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Greatest;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfNull;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Least;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.NullIf;
import org.elasticsearch.xpack.sql.parser.SqlParser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.sql.analysis.analyzer.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.sql.types.SqlTypesTests.loadMapping;

public class VerifierErrorMessagesTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();
    private final IndexResolution indexResolution = IndexResolution.valid(
        new EsIndex("test", loadMapping("mapping-multi-field-with-nested.json"))
    );

    private String error(String sql) {
        return error(indexResolution, sql);
    }

    private String error(IndexResolution getIndexResult, String sql) {
        Analyzer analyzer = analyzer(getIndexResult);
        VerificationException e = expectThrows(VerificationException.class, () -> analyzer.analyze(parser.createStatement(sql), true));
        String message = e.getMessage();
        assertTrue(message.startsWith("Found "));
        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    private LogicalPlan accept(String sql) {
        EsIndex test = getTestEsIndex();
        return accept(IndexResolution.valid(test), sql);
    }

    private EsIndex getTestEsIndex() {
        Map<String, EsField> mapping = loadMapping("mapping-multi-field-with-nested.json");
        return new EsIndex("test", mapping);
    }

    private LogicalPlan accept(IndexResolution resolution, String sql) {
        Analyzer analyzer = analyzer(resolution);
        return analyzer.analyze(parser.createStatement(sql), true);
    }

    private IndexResolution incompatible() {
        Map<String, EsField> basicMapping = loadMapping("mapping-basic.json", true);
        Map<String, EsField> incompatible = loadMapping("mapping-basic-incompatible.json");

        assertNotEquals(basicMapping, incompatible);
        IndexResolution resolution = IndexResolverTests.merge(
            new EsIndex("basic", basicMapping),
            new EsIndex("incompatible", incompatible)
        );
        assertTrue(resolution.isValid());
        return resolution;
    }

    private String incompatibleError(String sql) {
        return error(incompatible(), sql);
    }

    private LogicalPlan incompatibleAccept(String sql) {
        return accept(incompatible(), sql);
    }

    public void testMissingIndex() {
        assertEquals("1:17: Unknown index [missing]", error(IndexResolution.notFound("missing"), "SELECT foo FROM missing"));
    }

    public void testNonBooleanFilter() {
        Map<String, List<String>> testData = new HashMap<>();
        testData.put("INTEGER", List.of("int", "int + 1", "ABS(int)", "ASCII(keyword)"));
        testData.put("KEYWORD", List.of("keyword", "RTRIM(keyword)", "IIF(true, 'true', 'false')"));
        testData.put("DATETIME", List.of("date", "date + INTERVAL 1 DAY", "NOW()"));
        for (String typeName : testData.keySet()) {
            for (String exp : testData.get(typeName)) {
                assertEquals(
                    "1:26: Condition expression needs to be boolean, found [" + typeName + "]",
                    error("SELECT * FROM test WHERE " + exp)
                );
            }
        }
    }

    public void testMissingColumn() {
        assertEquals("1:8: Unknown column [xxx]", error("SELECT xxx FROM test"));
    }

    public void testMissingColumnFilter() {
        assertEquals("1:26: Unknown column [xxx]", error("SELECT * FROM test WHERE xxx > 1"));
    }

    public void testMissingColumnWithWildcard() {
        assertEquals("1:8: Unknown column [xxx]", error("SELECT xxx.* FROM test"));
    }

    public void testMisspelledColumnWithWildcard() {
        assertEquals("1:8: Unknown column [tex], did you mean [text]?", error("SELECT tex.* FROM test"));
    }

    public void testColumnWithNoSubFields() {
        assertEquals("1:8: Cannot determine columns for [text.*]", error("SELECT text.* FROM test"));
    }

    public void testFieldAliasTypeWithoutHierarchy() {
        Map<String, EsField> mapping = new LinkedHashMap<>();

        mapping.put("field", new EsField("field", OBJECT, singletonMap("alias", new EsField("alias", KEYWORD, emptyMap(), true)), false));

        IndexResolution resolution = IndexResolution.valid(new EsIndex("test", mapping));

        // check the nested alias is seen
        accept(resolution, "SELECT field.alias FROM test");
        // or its hierarhcy
        accept(resolution, "SELECT field.* FROM test");

        // check typos
        assertEquals("1:8: Unknown column [field.alas], did you mean [field.alias]?", error(resolution, "SELECT field.alas FROM test"));

        // non-existing parents for aliases are not seen by the user
        assertEquals("1:8: Cannot use field [field] type [object] only its subfields", error(resolution, "SELECT field FROM test"));
    }

    public void testMultipleColumnsWithWildcard1() {
        assertEquals("""
            1:14: Unknown column [a]
            line 1:17: Unknown column [b]
            line 1:22: Unknown column [c]
            line 1:25: Unknown column [tex], did you mean [text]?""", error("SELECT bool, a, b.*, c, tex.* FROM test"));
    }

    public void testMultipleColumnsWithWildcard2() {
        assertEquals("""
            1:8: Unknown column [tex], did you mean [text]?
            line 1:21: Unknown column [a]
            line 1:24: Unknown column [dat], did you mean [date]?
            line 1:31: Unknown column [c]""", error("SELECT tex.*, bool, a, dat.*, c FROM test"));
    }

    public void testMultipleColumnsWithWildcard3() {
        assertEquals("""
            1:8: Unknown column [ate], did you mean [date]?
            line 1:21: Unknown column [keyw], did you mean [keyword]?
            line 1:29: Unknown column [da], did you mean [date]?""", error("SELECT ate.*, bool, keyw.*, da FROM test"));
    }

    public void testMisspelledColumn() {
        assertEquals("1:8: Unknown column [txt], did you mean [text]?", error("SELECT txt FROM test"));
    }

    public void testFunctionOverMissingField() {
        assertEquals("1:12: Unknown column [xxx]", error("SELECT ABS(xxx) FROM test"));
    }

    public void testFunctionOverMissingFieldInFilter() {
        assertEquals("1:30: Unknown column [xxx]", error("SELECT * FROM test WHERE ABS(xxx) > 1"));
    }

    public void testMissingFunction() {
        assertEquals("1:8: Unknown function [ZAZ]", error("SELECT ZAZ(bool) FROM test"));
    }

    public void testMisspelledFunction() {
        assertEquals("1:8: Unknown function [COONT], did you mean any of [COUNT, COT, CONCAT]?", error("SELECT COONT(bool) FROM test"));
    }

    public void testMissingColumnInGroupBy() {
        assertEquals("1:41: Unknown column [xxx]", error("SELECT * FROM test GROUP BY DAY_OF_YEAR(xxx)"));
    }

    public void testInvalidOrdinalInOrderBy() {
        assertEquals(
            "1:56: Invalid ordinal [3] specified in [ORDER BY 2, 3] (valid range is [1, 2])",
            error("SELECT bool, MIN(int) FROM test GROUP BY 1 ORDER BY 2, 3")
        );
    }

    public void testFilterOnUnknownColumn() {
        assertEquals("1:26: Unknown column [xxx]", error("SELECT * FROM test WHERE xxx = 1"));
    }

    public void testMissingColumnInOrderBy() {
        assertEquals("1:29: Unknown column [xxx]", error("SELECT * FROM test ORDER BY xxx"));
    }

    public void testMissingColumnFunctionInOrderBy() {
        assertEquals("1:41: Unknown column [xxx]", error("SELECT * FROM test ORDER BY DAY_oF_YEAR(xxx)"));
    }

    public void testMissingExtract() {
        assertEquals("1:8: Unknown datetime field [ZAZ]", error("SELECT EXTRACT(ZAZ FROM date) FROM test"));
    }

    public void testMissingExtractSimilar() {
        assertEquals("1:8: Unknown datetime field [DAP], did you mean [DAY]?", error("SELECT EXTRACT(DAP FROM date) FROM test"));
    }

    public void testMissingExtractSimilarMany() {
        assertEquals(
            "1:8: Unknown datetime field [DOP], did you mean any of [DOM, DOW, DOY, IDOW]?",
            error("SELECT EXTRACT(DOP FROM date) FROM test")
        );
    }

    public void testExtractNonDateTime() {
        assertEquals("1:8: Invalid datetime field [ABS]. Use any datetime function.", error("SELECT EXTRACT(ABS FROM date) FROM test"));
    }

    public void testDateTruncValidArgs() {
        accept("SELECT DATE_TRUNC('decade', date) FROM test");
        accept("SELECT DATE_TRUNC('decades', date) FROM test");
        accept("SELECT DATETRUNC('day', date) FROM test");
        accept("SELECT DATETRUNC('days', date) FROM test");
        accept("SELECT DATE_TRUNC('dd', date) FROM test");
        accept("SELECT DATE_TRUNC('d', date) FROM test");
    }

    public void testDateTruncInvalidArgs() {
        assertEquals(
            "1:8: first argument of [DATE_TRUNC(int, date)] must be [string], found value [int] type [integer]",
            error("SELECT DATE_TRUNC(int, date) FROM test")
        );
        assertEquals(
            "1:8: second argument of [DATE_TRUNC(keyword, keyword)] must be [date, datetime or an interval data type],"
                + " found value [keyword] type [keyword]",
            error("SELECT DATE_TRUNC(keyword, keyword) FROM test")
        );
        assertEquals(
            "1:8: first argument of [DATE_TRUNC('invalid', keyword)] must be one of [MILLENNIUM, CENTURY, DECADE, "
                + ""
                + "YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND] "
                + "or their aliases; found value ['invalid']",
            error("SELECT DATE_TRUNC('invalid', keyword) FROM test")
        );
        assertEquals(
            "1:8: Unknown value ['millenioum'] for first argument of [DATE_TRUNC('millenioum', keyword)]; "
                + "did you mean [millennium, millennia]?",
            error("SELECT DATE_TRUNC('millenioum', keyword) FROM test")
        );
        assertEquals(
            "1:8: Unknown value ['yyyz'] for first argument of [DATE_TRUNC('yyyz', keyword)]; " + "did you mean [yyyy, yy]?",
            error("SELECT DATE_TRUNC('yyyz', keyword) FROM test")
        );
    }

    public void testDateAddValidArgs() {
        accept("SELECT DATE_ADD('weekday', 0, date) FROM test");
        accept("SELECT DATEADD('dw', 20, date) FROM test");
        accept("SELECT TIMESTAMP_ADD('years', -10, date) FROM test");
        accept("SELECT TIMESTAMPADD('dayofyear', 123, date) FROM test");
        accept("SELECT DATE_ADD('dy', 30, date) FROM test");
        accept("SELECT DATE_ADD('ms', 1, date::date) FROM test");
    }

    public void testDateAddInvalidArgs() {
        assertEquals(
            "1:8: first argument of [DATE_ADD(int, int, date)] must be [string], found value [int] type [integer]",
            error("SELECT DATE_ADD(int, int, date) FROM test")
        );
        assertEquals(
            "1:8: second argument of [DATE_ADD(keyword, 1.2, date)] must be [integer], found value [1.2] " + "type [double]",
            error("SELECT DATE_ADD(keyword, 1.2, date) FROM test")
        );
        assertEquals(
            "1:8: third argument of [DATE_ADD(keyword, int, keyword)] must be [date or datetime], found value [keyword] "
                + "type [keyword]",
            error("SELECT DATE_ADD(keyword, int, keyword) FROM test")
        );
        assertEquals(
            "1:8: first argument of [DATE_ADD('invalid', int, date)] must be one of [YEAR, QUARTER, MONTH, DAYOFYEAR, "
                + "DAY, WEEK, WEEKDAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND] "
                + "or their aliases; found value ['invalid']",
            error("SELECT DATE_ADD('invalid', int, date) FROM test")
        );
        assertEquals(
            "1:8: Unknown value ['sacinds'] for first argument of [DATE_ADD('sacinds', int, date)]; " + "did you mean [seconds, second]?",
            error("SELECT DATE_ADD('sacinds', int, date) FROM test")
        );
        assertEquals(
            "1:8: Unknown value ['dz'] for first argument of [DATE_ADD('dz', int, date)]; " + "did you mean [dd, dw, dy, d]?",
            error("SELECT DATE_ADD('dz', int, date) FROM test")
        );
    }

    public void testDateDiffValidArgs() {
        accept("SELECT DATE_DIFF('weekday', date, date) FROM test");
        accept("SELECT DATEDIFF('dw', date::date, date) FROM test");
        accept("SELECT TIMESTAMP_DIFF('years', date, date) FROM test");
        accept("SELECT TIMESTAMPDIFF('dayofyear', date, date::date) FROM test");
        accept("SELECT DATE_DIFF('dy', date, date) FROM test");
        accept("SELECT DATE_DIFF('ms', date::date, date::date) FROM test");
    }

    public void testDateDiffInvalidArgs() {
        assertEquals(
            "1:8: first argument of [DATE_DIFF(int, date, date)] must be [string], found value [int] type [integer]",
            error("SELECT DATE_DIFF(int, date, date) FROM test")
        );
        assertEquals(
            "1:8: second argument of [DATE_DIFF(keyword, keyword, date)] must be [date or datetime], found value [keyword] "
                + "type [keyword]",
            error("SELECT DATE_DIFF(keyword, keyword, date) FROM test")
        );
        assertEquals(
            "1:8: third argument of [DATE_DIFF(keyword, date, keyword)] must be [date or datetime], found value [keyword] "
                + "type [keyword]",
            error("SELECT DATE_DIFF(keyword, date, keyword) FROM test")
        );
        assertEquals(
            "1:8: first argument of [DATE_DIFF('invalid', int, date)] must be one of [YEAR, QUARTER, MONTH, DAYOFYEAR, "
                + "DAY, WEEK, WEEKDAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND] "
                + "or their aliases; found value ['invalid']",
            error("SELECT DATE_DIFF('invalid', int, date) FROM test")
        );
        assertEquals(
            "1:8: Unknown value ['sacinds'] for first argument of [DATE_DIFF('sacinds', int, date)]; " + "did you mean [seconds, second]?",
            error("SELECT DATE_DIFF('sacinds', int, date) FROM test")
        );
        assertEquals(
            "1:8: Unknown value ['dz'] for first argument of [DATE_DIFF('dz', int, date)]; " + "did you mean [dd, dw, dy, d]?",
            error("SELECT DATE_DIFF('dz', int, date) FROM test")
        );
    }

    public void testDateFormatValidArgs() {
        accept("SELECT DATE_FORMAT(date, '%H:%i:%s.%f') FROM test");
        accept("SELECT DATE_FORMAT(date::date, '%m/%d/%Y') FROM test");
        accept("SELECT DATE_FORMAT(date::time, '%H:%i:%s') FROM test");
    }

    public void testDateFormatInvalidArgs() {
        assertEquals(
            "1:8: first argument of [DATE_FORMAT(int, keyword)] must be [date, time or datetime], found value [int] type [integer]",
            error("SELECT DATE_FORMAT(int, keyword) FROM test")
        );
        assertEquals(
            "1:8: second argument of [DATE_FORMAT(date, int)] must be [string], found value [int] type [integer]",
            error("SELECT DATE_FORMAT(date, int) FROM test")
        );
    }

    public void testDatePartInvalidArgs() {
        assertEquals(
            "1:8: first argument of [DATE_PART(int, date)] must be [string], found value [int] type [integer]",
            error("SELECT DATE_PART(int, date) FROM test")
        );
        assertEquals(
            "1:8: second argument of [DATE_PART(keyword, keyword)] must be [date or datetime], found value [keyword] " + "type [keyword]",
            error("SELECT DATE_PART(keyword, keyword) FROM test")
        );
        assertEquals(
            "1:8: first argument of [DATE_PART('invalid', keyword)] must be one of [YEAR, QUARTER, MONTH, DAYOFYEAR, "
                + "DAY, WEEK, WEEKDAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, TZOFFSET] "
                + "or their aliases; found value ['invalid']",
            error("SELECT DATE_PART('invalid', keyword) FROM test")
        );
        assertEquals(
            "1:8: Unknown value ['tzofset'] for first argument of [DATE_PART('tzofset', keyword)]; " + "did you mean [tzoffset]?",
            error("SELECT DATE_PART('tzofset', keyword) FROM test")
        );
        assertEquals(
            "1:8: Unknown value ['dz'] for first argument of [DATE_PART('dz', keyword)]; " + "did you mean [dd, tz, dw, dy, d]?",
            error("SELECT DATE_PART('dz', keyword) FROM test")
        );
    }

    public void testDatePartValidArgs() {
        accept("SELECT DATE_PART('weekday', date) FROM test");
        accept("SELECT DATEPART('dw', date) FROM test");
        accept("SELECT DATEPART('tz', date) FROM test");
        accept("SELECT DATE_PART('dayofyear', date) FROM test");
        accept("SELECT DATE_PART('dy', date) FROM test");
        accept("SELECT DATE_PART('ms', date) FROM test");
    }

    public void testDateTimeFormatValidArgs() {
        accept("SELECT DATETIME_FORMAT(date, 'HH:mm:ss.SSS VV') FROM test");
        accept("SELECT DATETIME_FORMAT(date::date, 'MM/dd/YYYY') FROM test");
        accept("SELECT DATETIME_FORMAT(date::time, 'HH:mm:ss Z') FROM test");
    }

    public void testDateTimeFormatInvalidArgs() {
        assertEquals(
            "1:8: first argument of [DATETIME_FORMAT(int, keyword)] must be [date, time or datetime], found value [int] type [integer]",
            error("SELECT DATETIME_FORMAT(int, keyword) FROM test")
        );
        assertEquals(
            "1:8: second argument of [DATETIME_FORMAT(date, int)] must be [string], found value [int] type [integer]",
            error("SELECT DATETIME_FORMAT(date, int) FROM test")
        );
    }

    public void testDateTimeParseValidArgs() {
        accept("SELECT DATETIME_PARSE(keyword, 'MM/dd/uuuu HH:mm:ss') FROM test");
        accept("SELECT DATETIME_PARSE('04/07/2020 10:20:30 Europe/Berlin', 'MM/dd/uuuu HH:mm:ss VV') FROM test");
    }

    public void testDateTimeParseInvalidArgs() {
        assertEquals(
            "1:8: first argument of [DATETIME_PARSE(int, keyword)] must be [string], found value [int] type [integer]",
            error("SELECT DATETIME_PARSE(int, keyword) FROM test")
        );
        assertEquals(
            "1:8: second argument of [DATETIME_PARSE(keyword, int)] must be [string], found value [int] type [integer]",
            error("SELECT DATETIME_PARSE(keyword, int) FROM test")
        );
    }

    public void testFormatValidArgs() {
        accept("SELECT FORMAT(date, 'HH:mm:ss.fff KK') FROM test");
        accept("SELECT FORMAT(date::date, 'MM/dd/YYYY') FROM test");
        accept("SELECT FORMAT(date::time, 'HH:mm:ss Z') FROM test");
    }

    public void testFormatInvalidArgs() {
        assertEquals(
            "1:8: first argument of [FORMAT(int, keyword)] must be [date, time or datetime], found value [int] type [integer]",
            error("SELECT FORMAT(int, keyword) FROM test")
        );
        assertEquals(
            "1:8: second argument of [FORMAT(date, int)] must be [string], found value [int] type [integer]",
            error("SELECT FORMAT(date, int) FROM test")
        );
    }

    public void testValidDateTimeFunctionsOnTime() {
        accept("SELECT HOUR_OF_DAY(CAST(date AS TIME)) FROM test");
        accept("SELECT MINUTE_OF_HOUR(CAST(date AS TIME)) FROM test");
        accept("SELECT MINUTE_OF_DAY(CAST(date AS TIME)) FROM test");
        accept("SELECT SECOND_OF_MINUTE(CAST(date AS TIME)) FROM test");
    }

    public void testInvalidDateTimeFunctionsOnTime() {
        assertEquals(
            "1:8: argument of [DAY_OF_YEAR(CAST(date AS TIME))] must be [date or datetime], "
                + "found value [CAST(date AS TIME)] type [time]",
            error("SELECT DAY_OF_YEAR(CAST(date AS TIME)) FROM test")
        );
    }

    public void testGroupByOnTimeNotAllowed() {
        assertEquals(
            "1:36: Function [CAST(date AS TIME)] with data type [time] cannot be used for grouping",
            error("SELECT count(*) FROM test GROUP BY CAST(date AS TIME)")
        );
    }

    public void testGroupByOnTimeWrappedWithScalar() {
        accept("SELECT count(*) FROM test GROUP BY MINUTE(CAST(date AS TIME))");
    }

    public void testHistogramOnTimeNotAllowed() {
        assertEquals(
            "1:8: first argument of [HISTOGRAM] must be [date, datetime or numeric], " + "found value [CAST(date AS TIME)] type [time]",
            error("SELECT HISTOGRAM(CAST(date AS TIME), INTERVAL 1 MONTH), COUNT(*) FROM test GROUP BY 1")
        );
    }

    public void testSubtractFromInterval() {
        assertEquals(
            "1:8: Cannot subtract a datetime[CAST('2000-01-01' AS DATETIME)] "
                + "from an interval[INTERVAL 1 MONTH]; do you mean the reverse?",
            error("SELECT INTERVAL 1 MONTH - CAST('2000-01-01' AS DATETIME)")
        );

        assertEquals(
            "1:8: Cannot subtract a time[CAST('12:23:56.789' AS TIME)] " + "from an interval[INTERVAL 1 MONTH]; do you mean the reverse?",
            error("SELECT INTERVAL 1 MONTH - CAST('12:23:56.789' AS TIME)")
        );
    }

    public void testAddIntervalAndNumberNotAllowed() {
        assertEquals("1:8: [+] has arguments with incompatible types [INTERVAL_DAY] and [INTEGER]", error("SELECT INTERVAL 1 DAY + 100"));
        assertEquals("1:8: [+] has arguments with incompatible types [INTEGER] and [INTERVAL_DAY]", error("SELECT 100 + INTERVAL 1 DAY"));
    }

    public void testSubtractIntervalAndNumberNotAllowed() {
        assertEquals(
            "1:8: [-] has arguments with incompatible types [INTERVAL_MINUTE] and [DOUBLE]",
            error("SELECT INTERVAL 10 MINUTE - 100.0")
        );
        assertEquals(
            "1:8: [-] has arguments with incompatible types [DOUBLE] and [INTERVAL_MINUTE]",
            error("SELECT 100.0 - INTERVAL 10 MINUTE")
        );
    }

    public void testMultiplyIntervalWithDecimalNotAllowed() {
        assertEquals(
            "1:8: [*] has arguments with incompatible types [INTERVAL_MONTH] and [DOUBLE]",
            error("SELECT INTERVAL 1 MONTH * 1.234")
        );
        assertEquals(
            "1:8: [*] has arguments with incompatible types [DOUBLE] and [INTERVAL_MONTH]",
            error("SELECT 1.234 * INTERVAL 1 MONTH")
        );
    }

    public void testMultipleColumns() {
        // We get only one message back because the messages are grouped by the node that caused the issue
        assertEquals("1:43: Unknown column [xxx]", error("SELECT xxx FROM test GROUP BY DAY_oF_YEAR(xxx)"));
    }

    // GROUP BY
    public void testGroupBySelectWithAlias() {
        assertNotNull(accept("SELECT int AS i FROM test GROUP BY i"));
    }

    public void testGroupBySelectWithAliasOrderOnActualField() {
        assertNotNull(accept("SELECT int AS i FROM test GROUP BY i ORDER BY int"));
    }

    public void testGroupBySelectNonGrouped() {
        assertEquals("1:8: Cannot use non-grouped column [text], expected [int]", error("SELECT text, int FROM test GROUP BY int"));
    }

    public void testGroupByFunctionSelectFieldFromGroupByFunction() {
        assertEquals("1:8: Cannot use non-grouped column [int], expected [ABS(int)]", error("SELECT int FROM test GROUP BY ABS(int)"));
    }

    public void testGroupByOrderByNonGrouped() {
        assertEquals(
            "1:50: Cannot order by non-grouped column [bool], expected [text]",
            error("SELECT MAX(int) FROM test GROUP BY text ORDER BY bool")
        );
    }

    public void testGroupByOrderByNonGrouped_WithHaving() {
        assertEquals(
            "1:71: Cannot order by non-grouped column [bool], expected [text]",
            error("SELECT MAX(int) FROM test GROUP BY text HAVING MAX(int) > 10 ORDER BY bool")
        );
    }

    public void testGroupByOrdinalPointingToAggregate() {
        assertEquals(
            "1:42: Ordinal [2] in [GROUP BY 2] refers to an invalid argument, aggregate function [MIN(int)]",
            error("SELECT bool, MIN(int) FROM test GROUP BY 2")
        );
    }

    public void testGroupByInvalidOrdinal() {
        assertEquals(
            "1:42: Invalid ordinal [3] specified in [GROUP BY 3] (valid range is [1, 2])",
            error("SELECT bool, MIN(int) FROM test GROUP BY 3")
        );
    }

    public void testGroupByNegativeOrdinal() {
        assertEquals(
            "1:42: Invalid ordinal [-1] specified in [GROUP BY -1] (valid range is [1, 2])",
            error("SELECT bool, MIN(int) FROM test GROUP BY -1")
        );
    }

    public void testGroupByOrderByAliasedInSelectAllowed() {
        LogicalPlan lp = accept("SELECT int i FROM test GROUP BY int ORDER BY i");
        assertNotNull(lp);
    }

    public void testGroupByOrderByScalarOverNonGrouped() {
        assertEquals(
            "1:50: Cannot order by non-grouped column [YEAR(date)], expected [text] or an aggregate function",
            error("SELECT MAX(int) FROM test GROUP BY text ORDER BY YEAR(date)")
        );
    }

    public void testGroupByOrderByFieldFromGroupByFunction() {
        assertEquals(
            "1:54: Cannot order by non-grouped column [int], expected [ABS(int)]",
            error("SELECT ABS(int) FROM test GROUP BY ABS(int) ORDER BY int")
        );
        assertEquals(
            "1:91: Cannot order by non-grouped column [c], expected [b] or an aggregate function",
            error("SELECT b, abs, 2 as c FROM (SELECT bool as b, ABS(int) abs FROM test) GROUP BY b ORDER BY c")
        );
    }

    public void testGroupByOrderByScalarOverNonGrouped_WithHaving() {
        assertEquals(
            "1:71: Cannot order by non-grouped column [YEAR(date)], expected [text] or an aggregate function",
            error("SELECT MAX(int) FROM test GROUP BY text HAVING MAX(int) > 10 ORDER BY YEAR(date)")
        );
    }

    public void testGroupByHavingNonGrouped() {
        assertEquals(
            "1:48: Cannot use HAVING filter on non-aggregate [int]; use WHERE instead",
            error("SELECT AVG(int) FROM test GROUP BY bool HAVING int > 10")
        );
        accept("SELECT AVG(int) FROM test GROUP BY bool HAVING AVG(int) > 2");
    }

    public void testGroupByAggregate() {
        assertEquals("1:36: Cannot use an aggregate [AVG] for grouping", error("SELECT AVG(int) FROM test GROUP BY AVG(int)"));
        assertEquals(
            "1:65: Cannot use an aggregate [AVG] for grouping",
            error("SELECT ROUND(AVG(int),2), AVG(int), COUNT(*) FROM test GROUP BY AVG(int) ORDER BY AVG(int)")
        );
    }

    public void testStarOnNested() {
        assertNotNull(accept("SELECT dep.* FROM test"));
    }

    public void testGroupByOnInexact() {
        assertEquals(
            "1:36: Field [text] of data type [text] cannot be used for grouping; "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT COUNT(*) FROM test GROUP BY text")
        );
    }

    public void testGroupByOnNested() {
        assertEquals(
            "1:38: Grouping isn't (yet) compatible with nested fields [dep.dep_id]",
            error("SELECT dep.dep_id FROM test GROUP BY dep.dep_id")
        );
        assertEquals(
            "1:8: Grouping isn't (yet) compatible with nested fields [dep.dep_id]",
            error("SELECT dep.dep_id AS a FROM test GROUP BY a")
        );
        assertEquals(
            "1:8: Grouping isn't (yet) compatible with nested fields [dep.dep_id]",
            error("SELECT dep.dep_id AS a FROM test GROUP BY 1")
        );
        assertEquals(
            "1:8: Grouping isn't (yet) compatible with nested fields [dep.dep_id, dep.start_date]",
            error("SELECT dep.dep_id AS a, dep.start_date AS b FROM test GROUP BY 1, 2")
        );
        assertEquals(
            "1:8: Grouping isn't (yet) compatible with nested fields [dep.dep_id, dep.start_date]",
            error("SELECT dep.dep_id AS a, dep.start_date AS b FROM test GROUP BY a, b")
        );
    }

    public void testHavingOnNested() {
        assertEquals(
            "1:51: HAVING isn't (yet) compatible with nested fields [dep.start_date]",
            error("SELECT int FROM test GROUP BY int HAVING AVG(YEAR(dep.start_date)) > 1980")
        );
        assertEquals(
            "1:22: HAVING isn't (yet) compatible with nested fields [dep.start_date]",
            error("SELECT int, AVG(YEAR(dep.start_date)) AS average FROM test GROUP BY int HAVING average > 1980")
        );
        assertEquals(
            "1:22: HAVING isn't (yet) compatible with nested fields [dep.start_date, dep.end_date]",
            error(
                "SELECT int, AVG(YEAR(dep.start_date)) AS a, MAX(MONTH(dep.end_date)) AS b "
                    + "FROM test GROUP BY int "
                    + "HAVING a > 1980 AND b < 10"
            )
        );
    }

    public void testWhereOnNested() {
        assertEquals(
            "1:33: WHERE isn't (yet) compatible with scalar functions on nested fields [dep.start_date]",
            error("SELECT int FROM test WHERE YEAR(dep.start_date) + 10 > 0")
        );
        assertEquals(
            "1:13: WHERE isn't (yet) compatible with scalar functions on nested fields [dep.start_date]",
            error("SELECT YEAR(dep.start_date) + 10 AS a FROM test WHERE int > 10 AND (int < 3 OR NOT (a > 5))")
        );
        accept("SELECT int FROM test WHERE dep.start_date > '2020-01-30'::date AND (int > 10 OR dep.end_date IS NULL)");
        accept(
            "SELECT int FROM test WHERE dep.start_date > '2020-01-30'::date AND (int > 10 OR dep.end_date IS NULL) "
                + "OR NOT(dep.start_date >= '2020-01-01')"
        );
        String operator = randomFrom("<", "<=");
        assertEquals(
            "1:46: WHERE isn't (yet) compatible with scalar functions on nested fields [dep.location]",
            error("SELECT geo_shape FROM test " + "WHERE ST_Distance(dep.location, ST_WKTToSQL('point (10 20)')) " + operator + " 25")
        );
    }

    public void testOrderByOnNested() {
        assertEquals(
            "1:36: ORDER BY isn't (yet) compatible with scalar functions on nested fields [dep.start_date]",
            error("SELECT int FROM test ORDER BY YEAR(dep.start_date) + 10")
        );
        assertEquals(
            "1:13: ORDER BY isn't (yet) compatible with scalar functions on nested fields [dep.start_date]",
            error("SELECT YEAR(dep.start_date) + 10  FROM test ORDER BY 1")
        );
        assertEquals(
            "1:13: ORDER BY isn't (yet) compatible with scalar functions on nested fields " + "[dep.start_date, dep.end_date]",
            error("SELECT YEAR(dep.start_date) + 10 AS a, MONTH(dep.end_date) - 10 as b FROM test ORDER BY 1, 2")
        );
        accept("SELECT int FROM test ORDER BY dep.start_date, dep.end_date");
    }

    public void testGroupByScalarFunctionWithAggOnTarget() {
        assertEquals("1:31: Cannot use an aggregate [AVG] for grouping", error("SELECT int FROM test GROUP BY AVG(int) + 2"));
    }

    public void testUnsupportedType() {
        assertEquals("1:8: Cannot use field [unsupported] with unsupported type [ip_range]", error("SELECT unsupported FROM test"));
    }

    public void testUnsupportedStarExpansion() {
        assertEquals("1:8: Cannot use field [unsupported] with unsupported type [ip_range]", error("SELECT unsupported.* FROM test"));
    }

    public void testUnsupportedTypeInFilter() {
        assertEquals(
            "1:26: Cannot use field [unsupported] with unsupported type [ip_range]",
            error("SELECT * FROM test WHERE unsupported > 1")
        );
    }

    public void testValidRootFieldWithUnsupportedChildren() {
        accept("SELECT x FROM test");
    }

    public void testUnsupportedTypeInHierarchy() {
        assertEquals(
            "1:8: Cannot use field [x.y.z.w] with unsupported type [foobar] in hierarchy (field [y])",
            error("SELECT x.y.z.w FROM test")
        );
        assertEquals(
            "1:8: Cannot use field [x.y.z.v] with unsupported type [foobar] in hierarchy (field [y])",
            error("SELECT x.y.z.v FROM test")
        );
        assertEquals(
            "1:8: Cannot use field [x.y.z] with unsupported type [foobar] in hierarchy (field [y])",
            error("SELECT x.y.z.* FROM test")
        );
        assertEquals("1:8: Cannot use field [x.y] with unsupported type [foobar]", error("SELECT x.y FROM test"));
    }

    public void testTermEqualityOnInexact() {
        assertEquals(
            "1:26: [text = 'value'] cannot operate on first argument field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT * FROM test WHERE text = 'value'")
        );
    }

    public void testTermEqualityOnAmbiguous() {
        assertEquals(
            "1:26: [some.ambiguous = 'value'] cannot operate on first argument field of data type [text]: "
                + "Multiple exact keyword candidates available for [ambiguous]; specify which one to use",
            error("SELECT * FROM test WHERE some.ambiguous = 'value'")
        );
    }

    public void testUnsupportedTypeInFunction() {
        assertEquals("1:12: Cannot use field [unsupported] with unsupported type [ip_range]", error("SELECT ABS(unsupported) FROM test"));
    }

    public void testUnsupportedTypeInOrder() {
        assertEquals(
            "1:29: Cannot use field [unsupported] with unsupported type [ip_range]",
            error("SELECT * FROM test ORDER BY unsupported")
        );
    }

    public void testInexactFieldInOrder() {
        assertEquals(
            "1:29: ORDER BY cannot be applied to field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT * FROM test ORDER BY text")
        );
    }

    public void testGroupByOrderByAggregate() {
        accept("SELECT AVG(int) a FROM test GROUP BY bool ORDER BY a");
    }

    public void testGroupByOrderByAggs() {
        accept("SELECT int FROM test GROUP BY int ORDER BY COUNT(*)");
    }

    public void testGroupByOrderByAggAndGroupedColumn() {
        accept("SELECT int FROM test GROUP BY int ORDER BY int, MAX(int)");
    }

    public void testGroupByOrderByNonAggAndNonGroupedColumn() {
        assertEquals(
            "1:44: Cannot order by non-grouped column [bool], expected [int]",
            error("SELECT int FROM test GROUP BY int ORDER BY bool")
        );
    }

    public void testGroupByOrderByScore() {
        assertEquals(
            "1:44: Cannot order by non-grouped column [SCORE()], expected [int] or an aggregate function",
            error("SELECT int FROM test GROUP BY int ORDER BY SCORE()")
        );
    }

    public void testGroupByWithRepeatedAliases() {
        accept("SELECT int as x, keyword as x, max(date) as a FROM test GROUP BY 1, 2");
        accept("SELECT int as x, keyword as x, max(date) as a FROM test GROUP BY int, keyword");
    }

    public void testHavingOnColumn() {
        assertEquals(
            "1:42: Cannot use HAVING filter on non-aggregate [int]; use WHERE instead",
            error("SELECT int FROM test GROUP BY int HAVING int > 2")
        );
    }

    public void testHavingOnScalar() {
        assertEquals(
            "1:42: Cannot use HAVING filter on non-aggregate [int]; use WHERE instead",
            error("SELECT int FROM test GROUP BY int HAVING 2 < ABS(int)")
        );
    }

    public void testInWithIncompatibleDataTypes() {
        assertEquals(
            "1:8: 1st argument of ['2000-02-02T00:00:00Z'::date IN ('02:02:02Z'::time)] must be [date], "
                + "found value ['02:02:02Z'::time] type [time]",
            error("SELECT '2000-02-02T00:00:00Z'::date IN ('02:02:02Z'::time)")
        );
    }

    public void testInWithFieldInListOfValues() {
        assertEquals(
            "1:26: Comparisons against fields are not (currently) supported; offender [int] in [int IN (1, int)]",
            error("SELECT * FROM test WHERE int IN (1, int)")
        );
    }

    public void testInOnFieldTextWithNoKeyword() {
        assertEquals(
            "1:26: [IN] cannot operate on field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT * FROM test WHERE text IN ('foo', 'bar')")
        );
    }

    public void testNotSupportedAggregateOnDate() {
        assertEquals(
            "1:8: argument of [AVG(date)] must be [numeric], found value [date] type [datetime]",
            error("SELECT AVG(date) FROM test")
        );
    }

    public void testInvalidTypeForStringFunction_WithOneArgString() {
        assertEquals("1:8: argument of [LENGTH(1)] must be [string], found value [1] type [integer]", error("SELECT LENGTH(1)"));
    }

    public void testInvalidTypeForStringFunction_WithOneArgNumeric() {
        String functionName = randomFrom(Arrays.asList(Char.class, Space.class)).getSimpleName().toUpperCase(Locale.ROOT);
        assertEquals(
            "1:8: argument of [" + functionName + "('foo')] must be [integer], found value ['foo'] type [keyword]",
            error("SELECT " + functionName + "('foo')")
        );
        assertEquals(
            "1:8: argument of [" + functionName + "(1.2)] must be [integer], found value [1.2] type [double]",
            error("SELECT " + functionName + "(1.2)")
        );
    }

    public void testInvalidTypeForNestedStringFunctions_WithOneArg() {
        assertEquals(
            "1:15: argument of [SPACE('foo')] must be [integer], found value ['foo'] type [keyword]",
            error("SELECT LENGTH(SPACE('foo'))")
        );
        assertEquals(
            "1:15: argument of [SPACE(1.2)] must be [integer], found value [1.2] type [double]",
            error("SELECT LENGTH(SPACE(1.2))")
        );
    }

    public void testInvalidTypeForNumericFunction_WithOneArg() {
        assertEquals("1:8: argument of [COS('foo')] must be [numeric], found value ['foo'] type [keyword]", error("SELECT COS('foo')"));
    }

    public void testInvalidTypeForBooleanFunction_WithOneArg() {
        assertEquals("1:8: argument of [NOT 'foo'] must be [boolean], found value ['foo'] type [keyword]", error("SELECT NOT 'foo'"));
    }

    public void testInvalidTypeForStringFunction_WithTwoArgs() {
        assertEquals(
            "1:8: first argument of [CONCAT(1, 'bar')] must be [string], found value [1] type [integer]",
            error("SELECT CONCAT(1, 'bar')")
        );
        assertEquals(
            "1:8: second argument of [CONCAT('foo', 2)] must be [string], found value [2] type [integer]",
            error("SELECT CONCAT('foo', 2)")
        );
    }

    public void testInvalidTypeForNumericFunction_WithTwoArgs() {
        String functionName = randomFrom(Arrays.asList(Round.class, Truncate.class)).getSimpleName().toUpperCase(Locale.ROOT);
        assertEquals(
            "1:8: first argument of [" + functionName + "('foo', 2)] must be [numeric], found value ['foo'] type [keyword]",
            error("SELECT " + functionName + "('foo', 2)")
        );
        assertEquals(
            "1:8: second argument of [" + functionName + "(1.2, 'bar')] must be [integer], found value ['bar'] type [keyword]",
            error("SELECT " + functionName + "(1.2, 'bar')")
        );
        assertEquals(
            "1:8: second argument of [" + functionName + "(1.2, 3.4)] must be [integer], found value [3.4] type [double]",
            error("SELECT " + functionName + "(1.2, 3.4)")
        );
    }

    public void testInvalidTypeForBooleanFuntion_WithTwoArgs() {
        assertEquals("1:8: first argument of [1 OR true] must be [boolean], found value [1] type [integer]", error("SELECT 1 OR true"));
        assertEquals("1:8: second argument of [true OR 2] must be [boolean], found value [2] type [integer]", error("SELECT true OR 2"));
    }

    public void testInvalidTypeForReplace() {
        assertEquals(
            "1:8: first argument of [REPLACE(1, 'foo', 'bar')] must be [string], found value [1] type [integer]",
            error("SELECT REPLACE(1, 'foo', 'bar')")
        );
        assertEquals(
            "1:8: [REPLACE(text, 'foo', 'bar')] cannot operate on first argument field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT REPLACE(text, 'foo', 'bar') FROM test")
        );

        assertEquals(
            "1:8: second argument of [REPLACE('foo', 2, 'bar')] must be [string], found value [2] type [integer]",
            error("SELECT REPLACE('foo', 2, 'bar')")
        );
        assertEquals(
            "1:8: [REPLACE('foo', text, 'bar')] cannot operate on second argument field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT REPLACE('foo', text, 'bar') FROM test")
        );

        assertEquals(
            "1:8: third argument of [REPLACE('foo', 'bar', 3)] must be [string], found value [3] type [integer]",
            error("SELECT REPLACE('foo', 'bar', 3)")
        );
        assertEquals(
            "1:8: [REPLACE('foo', 'bar', text)] cannot operate on third argument field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT REPLACE('foo', 'bar', text) FROM test")
        );
    }

    public void testInvalidTypeForSubString() {
        assertEquals(
            "1:8: first argument of [SUBSTRING(1, 2, 3)] must be [string], found value [1] type [integer]",
            error("SELECT SUBSTRING(1, 2, 3)")
        );
        assertEquals(
            "1:8: [SUBSTRING(text, 2, 3)] cannot operate on first argument field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT SUBSTRING(text, 2, 3) FROM test")
        );

        assertEquals(
            "1:8: second argument of [SUBSTRING('foo', 'bar', 3)] must be [integer], found value ['bar'] type [keyword]",
            error("SELECT SUBSTRING('foo', 'bar', 3)")
        );
        assertEquals(
            "1:8: second argument of [SUBSTRING('foo', 1.2, 3)] must be [integer], found value [1.2] type [double]",
            error("SELECT SUBSTRING('foo', 1.2, 3)")
        );

        assertEquals(
            "1:8: third argument of [SUBSTRING('foo', 2, 'bar')] must be [integer], found value ['bar'] type [keyword]",
            error("SELECT SUBSTRING('foo', 2, 'bar')")
        );
        assertEquals(
            "1:8: third argument of [SUBSTRING('foo', 2, 3.4)] must be [integer], found value [3.4] type [double]",
            error("SELECT SUBSTRING('foo', 2, 3.4)")
        );
    }

    public void testInvalidTypeForFunction_WithFourArgs() {
        assertEquals(
            "1:8: first argument of [INSERT(1, 1, 2, 'new')] must be [string], found value [1] type [integer]",
            error("SELECT INSERT(1, 1, 2, 'new')")
        );
        assertEquals(
            "1:8: second argument of [INSERT('text', 'foo', 2, 'new')] must be [numeric], found value ['foo'] type [keyword]",
            error("SELECT INSERT('text', 'foo', 2, 'new')")
        );
        assertEquals(
            "1:8: third argument of [INSERT('text', 1, 'bar', 'new')] must be [numeric], found value ['bar'] type [keyword]",
            error("SELECT INSERT('text', 1, 'bar', 'new')")
        );
        assertEquals(
            "1:8: fourth argument of [INSERT('text', 1, 2, 3)] must be [string], found value [3] type [integer]",
            error("SELECT INSERT('text', 1, 2, 3)")
        );
    }

    public void testInvalidTypeForLikeMatch() {
        assertEquals(
            "1:26: [text LIKE 'foo'] cannot operate on field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT * FROM test WHERE text LIKE 'foo'")
        );
    }

    public void testInvalidTypeForRLikeMatch() {
        assertEquals(
            "1:26: [text RLIKE 'foo'] cannot operate on field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT * FROM test WHERE text RLIKE 'foo'")
        );
    }

    public void testMatchAndQueryFunctionsNotAllowedInSelect() {
        assertEquals(
            "1:8: Cannot use MATCH() or QUERY() full-text search functions in the SELECT clause",
            error("SELECT MATCH(text, 'foo') FROM test")
        );
        assertEquals(
            "1:8: Cannot use MATCH() or QUERY() full-text search functions in the SELECT clause",
            error("SELECT MATCH(text, 'foo') AS fullTextSearch FROM test")
        );
        assertEquals(
            "1:38: Cannot use MATCH() or QUERY() full-text search functions in the SELECT clause",
            error("SELECT int > 10 AND (bool = false OR QUERY('foo*')) AS fullTextSearch FROM test")
        );
        assertEquals(
            "1:8: Cannot use MATCH() or QUERY() full-text search functions in the SELECT clause\n"
                + "line 1:28: Cannot use MATCH() or QUERY() full-text search functions in the SELECT clause",
            error("SELECT MATCH(text, 'foo'), MATCH(text, 'bar') FROM test")
        );
        accept("SELECT * FROM test WHERE MATCH(text, 'foo')");
    }

    public void testAllowCorrectFieldsInIncompatibleMappings() {
        assertNotNull(incompatibleAccept("SELECT languages FROM \"*\""));
    }

    public void testWildcardInIncompatibleMappings() {
        assertNotNull(incompatibleAccept("SELECT * FROM \"*\""));
    }

    public void testMismatchedFieldInIncompatibleMappings() {
        assertEquals(
            "1:8: Cannot use field [emp_no] due to ambiguities being mapped as [2] incompatible types: "
                + "[integer] in [basic], [long] in [incompatible]",
            incompatibleError("SELECT emp_no FROM \"*\"")
        );
    }

    public void testMismatchedFieldStarInIncompatibleMappings() {
        assertEquals(
            "1:8: Cannot use field [emp_no] due to ambiguities being mapped as [2] incompatible types: "
                + "[integer] in [basic], [long] in [incompatible]",
            incompatibleError("SELECT emp_no.* FROM \"*\"")
        );
    }

    public void testMismatchedFieldFilterInIncompatibleMappings() {
        assertEquals(
            "1:33: Cannot use field [emp_no] due to ambiguities being mapped as [2] incompatible types: "
                + "[integer] in [basic], [long] in [incompatible]",
            incompatibleError("SELECT languages FROM \"*\" WHERE emp_no > 1")
        );
    }

    public void testMismatchedFieldScalarInIncompatibleMappings() {
        assertEquals(
            "1:45: Cannot use field [emp_no] due to ambiguities being mapped as [2] incompatible types: "
                + "[integer] in [basic], [long] in [incompatible]",
            incompatibleError("SELECT languages FROM \"*\" ORDER BY SIGN(ABS(emp_no))")
        );
    }

    public void testConditionalWithDifferentDataTypes() {
        @SuppressWarnings("unchecked")
        String function = randomFrom(IfNull.class, NullIf.class).getSimpleName();
        assertEquals(
            "1:17: 2nd argument of [" + function + "(3, '4')] must be [integer], found value ['4'] type [keyword]",
            error("SELECT 1 = 1 OR " + function + "(3, '4') > 1")
        );

        @SuppressWarnings("unchecked")
        String arbirtraryArgsFunction = randomFrom(Coalesce.class, Greatest.class, Least.class).getSimpleName();
        assertEquals(
            "1:17: 3rd argument of [" + arbirtraryArgsFunction + "(null, 3, '4')] must be [integer], " + "found value ['4'] type [keyword]",
            error("SELECT 1 = 1 OR " + arbirtraryArgsFunction + "(null, 3, '4') > 1")
        );
    }

    public void testCaseWithNonBooleanConditionExpression() {
        assertEquals(
            "1:8: condition of [WHEN abs(int) THEN 'foo'] must be [boolean], found value [abs(int)] type [integer]",
            error("SELECT CASE WHEN int = 1 THEN 'one' WHEN abs(int) THEN 'foo' END FROM test")
        );
    }

    public void testCaseWithDifferentResultDataTypes() {
        assertEquals(
            "1:8: result of [WHEN int > 10 THEN 10] must be [keyword], found value [10] type [integer]",
            error("SELECT CASE WHEN int > 20 THEN 'foo' WHEN int > 10 THEN 10 ELSE 'bar' END FROM test")
        );
    }

    public void testCaseWithDifferentResultAndDefaultValueDataTypes() {
        assertEquals(
            "1:8: ELSE clause of [date] must be [keyword], found value [date] type [datetime]",
            error("SELECT CASE WHEN int > 20 THEN 'foo' ELSE date END FROM test")
        );
    }

    public void testCaseWithDifferentResultAndDefaultValueDataTypesAndNullTypesSkipped() {
        assertEquals(
            "1:8: ELSE clause of [date] must be [keyword], found value [date] type [datetime]",
            error("SELECT CASE WHEN int > 20 THEN null WHEN int > 10 THEN null WHEN int > 5 THEN 'foo' ELSE date END FROM test")
        );
    }

    public void testIifWithNonBooleanConditionExpression() {
        assertEquals(
            "1:8: first argument of [IIF(int, 'one', 'zero')] must be [boolean], found value [int] type [integer]",
            error("SELECT IIF(int, 'one', 'zero') FROM test")
        );
    }

    public void testIifWithDifferentResultAndDefaultValueDataTypes() {
        assertEquals(
            "1:8: third argument of [IIF(int > 20, 'foo', date)] must be [keyword], found value [date] type [datetime]",
            error("SELECT IIF(int > 20, 'foo', date) FROM test")
        );
    }

    public void testAggsInWhere() {
        assertEquals(
            "1:33: Cannot use WHERE filtering on aggregate function [MAX(int)], use HAVING instead",
            error("SELECT MAX(int) FROM test WHERE MAX(int) > 10 GROUP BY bool")
        );
    }

    public void testHavingInAggs() {
        assertEquals(
            "1:29: [int] field must appear in the GROUP BY clause or in an aggregate function",
            error("SELECT int FROM test HAVING MAX(int) = 0")
        );

        assertEquals(
            "1:35: [int] field must appear in the GROUP BY clause or in an aggregate function",
            error("SELECT int FROM test HAVING int = count(1)")
        );
    }

    public void testHavingAsWhere() {
        // TODO: this query works, though it normally shouldn't; a check about it could only be enforced if the Filter would be qualified
        // (WHERE vs HAVING). Otoh, this "extra flexibility" shouldn't be harmful atp.
        accept("SELECT int FROM test HAVING int = 1");
        accept("SELECT int FROM test HAVING SIN(int) + 5 > 5.5");
        // HAVING's expression being AND'ed to WHERE's
        accept("SELECT int FROM test WHERE int > 3 HAVING POWER(int, 2) < 100");
    }

    public void testHistogramInFilter() {
        assertEquals(
            "1:63: Cannot filter on grouping function [HISTOGRAM(date, INTERVAL 1 MONTH)], use its argument instead",
            error(
                "SELECT HISTOGRAM(date, INTERVAL 1 MONTH) AS h FROM test WHERE "
                    + "HISTOGRAM(date, INTERVAL 1 MONTH) > CAST('2000-01-01' AS DATETIME) GROUP BY h"
            )
        );
    }

    // related https://github.com/elastic/elasticsearch/issues/36853
    public void testHistogramInHaving() {
        assertEquals(
            "1:75: Cannot filter on grouping function [h], use its argument instead",
            error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) AS h FROM test GROUP BY h HAVING " + "h > CAST('2000-01-01' AS DATETIME)")
        );
    }

    public void testGroupByScalarOnTopOfGrouping() {
        assertEquals(
            "1:14: Cannot combine [HISTOGRAM(date, INTERVAL 1 MONTH)] grouping function inside "
                + "GROUP BY, found [MONTH(HISTOGRAM(date, INTERVAL 1 MONTH))]; consider moving the expression inside the histogram",
            error("SELECT MONTH(HISTOGRAM(date, INTERVAL 1 MONTH)) AS h FROM test GROUP BY h")
        );
    }

    public void testAggsInHistogram() {
        assertEquals("1:37: Cannot use an aggregate [MAX] for grouping", error("SELECT MAX(date) FROM test GROUP BY MAX(int)"));
    }

    public void testGroupingsInHistogram() {
        assertEquals(
            "1:47: Cannot embed grouping functions within each other, found [HISTOGRAM(int, 1)] in [HISTOGRAM(HISTOGRAM(int, 1), 1)]",
            error("SELECT MAX(date) FROM test GROUP BY HISTOGRAM(HISTOGRAM(int, 1), 1)")
        );
    }

    public void testCastInHistogram() {
        accept("SELECT MAX(date) FROM test GROUP BY HISTOGRAM(CAST(int AS LONG), 1)");
    }

    public void testHistogramNotInGrouping() {
        assertEquals(
            "1:8: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
            error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) AS h FROM test")
        );
    }

    public void testHistogramNotInGroupingWithCount() {
        assertEquals(
            "1:8: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
            error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) AS h, COUNT(*) FROM test")
        );
    }

    public void testHistogramNotInGroupingWithMaxFirst() {
        assertEquals(
            "1:19: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
            error("SELECT MAX(date), HISTOGRAM(date, INTERVAL 1 MONTH) AS h FROM test")
        );
    }

    public void testHistogramWithoutAliasNotInGrouping() {
        assertEquals(
            "1:8: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
            error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) FROM test")
        );
    }

    public void testTwoHistogramsNotInGrouping() {
        assertEquals(
            "1:48: [HISTOGRAM(date, INTERVAL 1 DAY)] needs to be part of the grouping",
            error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) AS h, HISTOGRAM(date, INTERVAL 1 DAY) FROM test GROUP BY h")
        );
    }

    public void testHistogramNotInGrouping_WithGroupByField() {
        assertEquals(
            "1:8: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
            error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) FROM test GROUP BY date")
        );
    }

    public void testScalarOfHistogramNotInGrouping() {
        assertEquals(
            "1:14: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
            error("SELECT MONTH(HISTOGRAM(date, INTERVAL 1 MONTH)) FROM test")
        );
    }

    public void testErrorMessageForPercentileWithSecondArgBasedOnAField() {
        assertEquals(
            "1:8: second argument of [PERCENTILE(int, ABS(int))] must be a constant, received [ABS(int)]",
            error("SELECT PERCENTILE(int, ABS(int)) FROM test")
        );
    }

    public void testErrorMessageForPercentileWithWrongMethodType() {
        assertEquals(
            "1:8: third argument of [PERCENTILE(int, 50, 2)] must be [string], found value [2] type [integer]",
            error("SELECT PERCENTILE(int, 50, 2) FROM test")
        );
    }

    public void testErrorMessageForPercentileWithNullMethodType() {
        assertEquals(
            "1:8: third argument of [PERCENTILE(int, 50, null)] must be one of [tdigest, hdr], received [null]",
            error("SELECT PERCENTILE(int, 50, null) FROM test")
        );
    }

    public void testErrorMessageForPercentileWithHDRRequiresInt() {
        assertEquals(
            "1:8: fourth argument of [PERCENTILE(int, 50, 'hdr', 2.2)] must be [integer], found value [2.2] type [double]",
            error("SELECT PERCENTILE(int, 50, 'hdr', 2.2) FROM test")
        );
    }

    public void testErrorMessageForPercentileWithWrongMethod() {
        assertEquals(
            "1:8: third argument of [PERCENTILE(int, 50, 'notExistingMethod', 5)] must be "
                + "one of [tdigest, hdr], received [notExistingMethod]",
            error("SELECT PERCENTILE(int, 50, 'notExistingMethod', 5) FROM test")
        );
    }

    public void testErrorMessageForPercentileWithWrongMethodParameterType() {
        assertEquals(
            "1:8: fourth argument of [PERCENTILE(int, 50, 'tdigest', '5')] must be [numeric], found value ['5'] type [keyword]",
            error("SELECT PERCENTILE(int, 50, 'tdigest', '5') FROM test")
        );
    }

    public void testErrorMessageForPercentileRankWithSecondArgBasedOnAField() {
        assertEquals(
            "1:8: second argument of [PERCENTILE_RANK(int, ABS(int))] must be a constant, received [ABS(int)]",
            error("SELECT PERCENTILE_RANK(int, ABS(int)) FROM test")
        );
    }

    public void testErrorMessageForPercentileRankWithWrongMethodType() {
        assertEquals(
            "1:8: third argument of [PERCENTILE_RANK(int, 50, 2)] must be [string], found value [2] type [integer]",
            error("SELECT PERCENTILE_RANK(int, 50, 2) FROM test")
        );
    }

    public void testErrorMessageForPercentileRankWithNullMethodType() {
        assertEquals(
            "1:8: third argument of [PERCENTILE_RANK(int, 50, null)] must be one of [tdigest, hdr], received [null]",
            error("SELECT PERCENTILE_RANK(int, 50, null) FROM test")
        );
    }

    public void testErrorMessageForPercentileRankWithHDRRequiresInt() {
        assertEquals(
            "1:8: fourth argument of [PERCENTILE_RANK(int, 50, 'hdr', 2.2)] must be [integer], found value [2.2] type [double]",
            error("SELECT PERCENTILE_RANK(int, 50, 'hdr', 2.2) FROM test")
        );
    }

    public void testErrorMessageForPercentileRankWithWrongMethod() {
        assertEquals(
            "1:8: third argument of [PERCENTILE_RANK(int, 50, 'notExistingMethod', 5)] must be "
                + "one of [tdigest, hdr], received [notExistingMethod]",
            error("SELECT PERCENTILE_RANK(int, 50, 'notExistingMethod', 5) FROM test")
        );
    }

    public void testErrorMessageForPercentileRankWithWrongMethodParameterType() {
        assertEquals(
            "1:8: fourth argument of [PERCENTILE_RANK(int, 50, 'tdigest', '5')] must be [numeric], " + "found value ['5'] type [keyword]",
            error("SELECT PERCENTILE_RANK(int, 50, 'tdigest', '5') FROM test")
        );
    }

    public void testTopHitsFirstArgConstant() {
        String topHitsFunction = randomTopHitsFunction();
        assertEquals(
            "1:8: first argument of [" + topHitsFunction + "('foo', int)] must be a table column, found constant ['foo']",
            error("SELECT " + topHitsFunction + "('foo', int) FROM test")
        );
    }

    public void testTopHitsSecondArgConstant() {
        String topHitsFunction = randomTopHitsFunction();
        assertEquals(
            "1:8: second argument of [" + topHitsFunction + "(int, 10)] must be a table column, found constant [10]",
            error("SELECT " + topHitsFunction + "(int, 10) FROM test")
        );
    }

    public void testTopHitsFirstArgTextWithNoKeyword() {
        String topHitsFunction = randomTopHitsFunction();
        assertEquals(
            "1:8: ["
                + topHitsFunction
                + "(text)] cannot operate on first argument field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT " + topHitsFunction + "(text) FROM test")
        );
    }

    public void testTopHitsSecondArgTextWithNoKeyword() {
        String topHitsFunction = randomTopHitsFunction();
        assertEquals(
            "1:8: ["
                + topHitsFunction
                + "(keyword, text)] cannot operate on second argument field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT " + topHitsFunction + "(keyword, text) FROM test")
        );
    }

    public void testTopHitsByHavingUnsupported() {
        String topHitsFunction = randomTopHitsFunction();
        int column = 31 + topHitsFunction.length();
        assertEquals(
            "1:" + column + ": filtering is unsupported for function [" + topHitsFunction + "(int)]",
            error("SELECT " + topHitsFunction + "(int) FROM test HAVING " + topHitsFunction + "(int) > 10")
        );
    }

    public void testTopHitsGroupByHavingUnsupported() {
        String topHitsFunction = randomTopHitsFunction();
        int column = 45 + topHitsFunction.length();
        assertEquals(
            "1:" + column + ": filtering is unsupported for function [" + topHitsFunction + "(int)]",
            error("SELECT " + topHitsFunction + "(int) FROM test GROUP BY text HAVING " + topHitsFunction + "(int) > 10")
        );
    }

    public void testTopHitsHavingWithSubqueryUnsupported() {
        String filter = randomFrom("WHERE", "HAVING");
        int column = 99 + filter.length();
        assertEquals(
            "1:" + column + ": filtering is unsupported for functions [FIRST(int), LAST(int)]",
            error(
                "SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT FIRST(int) AS f, LAST(int) AS l FROM test))) "
                    + filter
                    + " f > 10 or l < 10"
            )
        );
    }

    public void testTopHitsGroupByHavingWithSubqueryUnsupported() {
        String filter = randomFrom("WHERE", "HAVING");
        int column = 113 + filter.length();
        assertEquals(
            "1:" + column + ": filtering is unsupported for functions [FIRST(int), LAST(int)]",
            error(
                "SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT FIRST(int) AS f, LAST(int) AS l FROM test GROUP BY bool))) "
                    + filter
                    + " f > 10 or l < 10"
            )
        );
    }

    public void testMinOnInexactUnsupported() {
        assertEquals(
            "1:8: [MIN(text)] cannot operate on field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT MIN(text) FROM test")
        );
    }

    public void testMaxOnInexactUnsupported() {
        assertEquals(
            "1:8: [MAX(text)] cannot operate on field of data type [text]: "
                + "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT MAX(text) FROM test")
        );
    }

    public void testMinOnKeywordGroupByHavingUnsupported() {
        assertEquals(
            "1:52: HAVING filter is unsupported for function [MIN(keyword)]",
            error("SELECT MIN(keyword) FROM test GROUP BY text HAVING MIN(keyword) > 10")
        );
    }

    public void testMaxOnKeywordGroupByHavingUnsupported() {
        assertEquals(
            "1:52: HAVING filter is unsupported for function [MAX(keyword)]",
            error("SELECT MAX(keyword) FROM test GROUP BY text HAVING MAX(keyword) > 10")
        );
    }

    public void testMinOnUnsignedLongGroupByHavingUnsupported() {
        assertEquals(
            "1:62: HAVING filter is unsupported for function [MIN(unsigned_long)]",
            error("SELECT MIN(unsigned_long) min FROM test GROUP BY text HAVING min > 10")
        );
    }

    public void testMaxOnUnsignedLongGroupByHavingUnsupported() {
        assertEquals(
            "1:62: HAVING filter is unsupported for function [MAX(unsigned_long)]",
            error("SELECT MAX(unsigned_long) max FROM test GROUP BY text HAVING max > 10")
        );
    }

    public void testProjectAliasInFilter() {
        accept("SELECT int AS i FROM test WHERE i > 10");
    }

    public void testAggregateAliasInFilter() {
        accept("SELECT int AS i FROM test WHERE i > 10 GROUP BY i HAVING MAX(i) > 10");
    }

    public void testProjectUnresolvedAliasInFilter() {
        assertEquals("1:8: Unknown column [tni]", error("SELECT tni AS i FROM test WHERE i > 10 GROUP BY i"));
    }

    public void testProjectUnresolvedAliasWithSameNameInFilter() {
        assertEquals("1:8: Unknown column [i]", error("SELECT i AS i FROM test WHERE i > 10 GROUP BY i"));
    }

    public void testProjectUnresolvedAliasWithSameNameInOrderBy() {
        assertEquals("1:8: Unknown column [i]", error("SELECT i AS i FROM test ORDER BY i"));
    }

    public void testGeoShapeInWhereClause() {
        assertEquals(
            "1:53: geo shapes cannot be used for filtering",
            error("SELECT ST_AsWKT(geo_shape) FROM test WHERE ST_AsWKT(geo_shape) = 'point (10 20)'")
        );

        // We get only one message back because the messages are grouped by the node that caused the issue
        assertEquals(
            "1:50: geo shapes cannot be used for filtering",
            error(
                "SELECT MAX(ST_X(geo_shape)) FROM test WHERE ST_Y(geo_shape) > 10 "
                    + "GROUP BY ST_GEOMETRYTYPE(geo_shape) ORDER BY ST_ASWKT(geo_shape)"
            )
        );
    }

    public void testGeoShapeInGroupBy() {
        assertEquals("1:48: geo shapes cannot be used in grouping", error("SELECT ST_X(geo_shape) FROM test GROUP BY ST_X(geo_shape)"));
    }

    public void testGeoShapeInOrderBy() {
        assertEquals("1:48: geo shapes cannot be used for sorting", error("SELECT ST_X(geo_shape) FROM test ORDER BY ST_Z(geo_shape)"));
    }

    public void testGeoShapeInSelect() {
        accept("SELECT ST_X(geo_shape) FROM test");
    }

    //
    // Pivot verifications
    //
    public void testPivotNonExactColumn() {
        assertEquals(
            "1:72: Field [text] of data type [text] cannot be used for grouping;"
                + " No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT * FROM (SELECT int, text, keyword FROM test) " + "PIVOT(AVG(int) FOR text IN ('bla'))")
        );
    }

    public void testPivotColumnUsedInsteadOfAgg() {
        assertEquals(
            "1:59: No aggregate function found in PIVOT at [int]",
            error("SELECT * FROM (SELECT int, keyword, bool FROM test) " + "PIVOT(int FOR keyword IN ('bla'))")
        );
    }

    public void testPivotScalarUsedInsteadOfAgg() {
        assertEquals(
            "1:59: No aggregate function found in PIVOT at [ROUND(int)]",
            error("SELECT * FROM (SELECT int, keyword, bool FROM test) " + "PIVOT(ROUND(int) FOR keyword IN ('bla'))")
        );
    }

    public void testPivotScalarUsedAlongSideAgg() {
        assertEquals(
            "1:59: Non-aggregate function found in PIVOT at [AVG(int) + ROUND(int)]",
            error("SELECT * FROM (SELECT int, keyword, bool FROM test) " + "PIVOT(AVG(int) + ROUND(int) FOR keyword IN ('bla'))")
        );
    }

    public void testPivotValueNotFoldable() {
        assertEquals(
            "1:91: Non-literal [bool] found inside PIVOT values",
            error("SELECT * FROM (SELECT int, keyword, bool FROM test) " + "PIVOT(AVG(int) FOR keyword IN ('bla', bool))")
        );
    }

    public void testPivotWithFunctionInput() {
        assertEquals(
            "1:37: No functions allowed (yet); encountered [YEAR(date)]",
            error("SELECT * FROM (SELECT int, keyword, YEAR(date) FROM test) " + "PIVOT(AVG(int) FOR keyword IN ('bla'))")
        );
    }

    public void testPivotWithFoldableFunctionInValues() {
        assertEquals(
            "1:85: Non-literal [UCASE('bla')] found inside PIVOT values",
            error("SELECT * FROM (SELECT int, keyword, bool FROM test) " + "PIVOT(AVG(int) FOR keyword IN ( UCASE('bla') ))")
        );
    }

    public void testPivotWithNull() {
        assertEquals(
            "1:85: Null not allowed as a PIVOT value",
            error("SELECT * FROM (SELECT int, keyword, bool FROM test) " + "PIVOT(AVG(int) FOR keyword IN ( null ))")
        );
    }

    public void testPivotValuesHaveDifferentTypeThanColumn() {
        assertEquals(
            "1:81: Literal ['bla'] of type [keyword] does not match type [boolean] of PIVOT column [bool]",
            error("SELECT * FROM (SELECT int, keyword, bool FROM test) " + "PIVOT(AVG(int) FOR bool IN ('bla'))")
        );
    }

    public void testPivotValuesWithMultipleDifferencesThanColumn() {
        assertEquals(
            "1:81: Literal ['bla'] of type [keyword] does not match type [boolean] of PIVOT column [bool]",
            error("SELECT * FROM (SELECT int, keyword, bool FROM test) " + "PIVOT(AVG(int) FOR bool IN ('bla', true))")
        );
    }

    public void testErrorMessageForMatrixStatsWithScalars() {
        assertEquals(
            "1:17: [KURTOSIS()] cannot be used on top of operators or scalars",
            error("SELECT KURTOSIS(ABS(int * 10.123)) FROM test")
        );
        assertEquals(
            "1:17: [SKEWNESS()] cannot be used on top of operators or scalars",
            error("SELECT SKEWNESS(ABS(int * 10.123)) FROM test")
        );
    }

    public void testCastOnInexact() {
        // inexact with underlying keyword
        assertEquals(
            "1:36: [some.string] of data type [text] cannot be used for [CAST()] inside the WHERE clause",
            error("SELECT * FROM test WHERE NOT (CAST(some.string AS string) = 'foo') OR true")
        );
        // inexact without underlying keyword (text only)
        assertEquals(
            "1:36: [text] of data type [text] cannot be used for [CAST()] inside the WHERE clause",
            error("SELECT * FROM test WHERE NOT (CAST(text AS string) = 'foo') OR true")
        );
    }

    public void testBinaryFieldWithDocValues() {
        accept("SELECT binary_stored FROM test WHERE binary_stored IS NOT NULL");
        accept("SELECT binary_stored FROM test GROUP BY binary_stored HAVING count(binary_stored) > 1");
        accept("SELECT count(binary_stored) FROM test HAVING count(binary_stored) > 1");
        accept("SELECT binary_stored FROM test ORDER BY binary_stored");
    }

    public void testBinaryFieldWithNoDocValues() {
        assertEquals(
            "1:31: Binary field [binary] cannot be used for filtering unless it has the doc_values setting enabled",
            error("SELECT binary FROM test WHERE binary IS NOT NULL")
        );
        assertEquals(
            "1:34: Binary field [binary] cannot be used in aggregations unless it has the doc_values setting enabled",
            error("SELECT binary FROM test GROUP BY binary")
        );
        assertEquals(
            "1:45: Binary field [binary] cannot be used for filtering unless it has the doc_values setting enabled",
            error("SELECT count(binary) FROM test HAVING count(binary) > 1")
        );
        assertEquals(
            "1:34: Binary field [binary] cannot be used for ordering unless it has the doc_values setting enabled",
            error("SELECT binary FROM test ORDER BY binary")
        );
    }

    public void testDistinctIsNotSupported() {
        assertEquals("1:8: SELECT DISTINCT is not yet supported", error("SELECT DISTINCT int FROM test"));
    }

    public void testExistsIsNotSupported() {
        assertEquals("1:33: EXISTS is not yet supported", error("SELECT test.int FROM test WHERE EXISTS (SELECT 1)"));
    }

    public void testScoreCannotBeUsedInExpressions() {
        assertEquals(
            "1:12: [SCORE()] cannot be used in expressions, does not support further processing",
            error("SELECT SIN(SCORE()) FROM test")
        );
    }

    public void testScoreIsNotInHaving() {
        assertEquals(
            "1:54: HAVING filter is unsupported for function [SCORE()]\n"
                + "line 1:54: [SCORE()] cannot be used in expressions, does not support further processing",
            error("SELECT bool, AVG(int) FROM test GROUP BY bool HAVING SCORE() > 0.5")
        );
    }

    public void testScoreCannotBeUsedForGrouping() {
        assertEquals("1:42: Cannot use [SCORE()] for grouping", error("SELECT bool, AVG(int) FROM test GROUP BY SCORE()"));
    }

    public void testScoreCannotBeAnAggregateFunction() {
        assertEquals(
            "1:14: Cannot use non-grouped column [SCORE()], expected [bool]",
            error("SELECT bool, SCORE() FROM test GROUP BY bool")
        );
    }

    public void testScalarFunctionInAggregateAndGrouping() {
        accept("SELECT bool, SQRT(int) FROM test GROUP BY bool, SQRT(int)");
        accept("SELECT bool, SQRT(int) FROM test GROUP BY bool, int");
    }

    public void testFunctionInAggregateAndGroupingAsReference() {
        accept("SELECT b, s FROM (SELECT bool b, int, SQRT(int) s FROM test) GROUP BY b, SQRT(int)");
    }

    public void testLiteralAsAggregate() {
        accept("SELECT bool, AVG(int), 2 as lit FROM test GROUP BY bool");
    }

    public void testShapeInWhereClause() {
        assertEquals(
            "1:49: shapes cannot be used for filtering",
            error("SELECT ST_AsWKT(shape) FROM test WHERE ST_AsWKT(shape) = 'point (10 20)'")
        );
        assertEquals(
            "1:46: shapes cannot be used for filtering",
            error("SELECT MAX(ST_X(shape)) FROM test WHERE ST_Y(shape) > 10 GROUP BY ST_GEOMETRYTYPE(shape) ORDER BY ST_ASWKT(shape)")
        );
    }

    public void testShapeInGroupBy() {
        assertEquals("1:44: shapes cannot be used in grouping", error("SELECT ST_X(shape) FROM test GROUP BY ST_X(shape)"));
    }

    public void testShapeInOrderBy() {
        assertEquals("1:44: shapes cannot be used for sorting", error("SELECT ST_X(shape) FROM test ORDER BY ST_Z(shape)"));
    }

    public void testShapeInSelect() {
        accept("SELECT ST_X(shape) FROM test");
    }

    public void testSubselectWhereOnGroupBy() {
        accept("SELECT b, a FROM (SELECT bool as b, AVG(int) as a FROM test GROUP BY bool) WHERE b = false");
        accept("SELECT b, a FROM (SELECT bool as b, AVG(int) as a FROM test GROUP BY bool HAVING AVG(int) > 2) WHERE b = false");
    }

    public void testSubselectWhereOnAggregate() {
        accept("SELECT b, a FROM (SELECT bool as b, AVG(int) as a FROM test GROUP BY bool) WHERE a > 10");
        accept("SELECT b, a FROM (SELECT bool as b, AVG(int) as a FROM test GROUP BY bool) WHERE a > 10 AND b = FALSE");
    }

    public void testSubselectWithOrderWhereOnAggregate() {
        accept("SELECT * FROM (SELECT bool as b, AVG(int) as a FROM test GROUP BY bool ORDER BY bool) WHERE a > 10");
    }

    public void testNestedAggregate() {
        Consumer<String> checkMsg = (String sql) -> {
            var actual = error(sql);
            assertTrue(actual, actual.contains("Nested aggregations in sub-selects are not supported."));
        };

        checkMsg.accept("SELECT SUM(c) FROM (SELECT COUNT(*) c FROM test)");
        checkMsg.accept("SELECT COUNT(*) FROM (SELECT SUM(int) c FROM test)");
        checkMsg.accept("SELECT i FROM (SELECT int i FROM test GROUP BY i) GROUP BY i");
        checkMsg.accept("SELECT c FROM (SELECT SUM(int) c FROM test) GROUP BY c HAVING COUNT(*) > 10");
        checkMsg.accept("SELECT COUNT(*) FROM (SELECT int i FROM test GROUP BY i)");
        checkMsg.accept("SELECT a.i, COUNT(a.c) FROM (SELECT int i, COUNT(int) c FROM test GROUP BY int) a GROUP BY c");
        // directly related to https://github.com/elastic/elasticsearch/issues/81577
        // before the fix, this query was throwing a StackOverflowError
        checkMsg.accept(
            "SELECT MAX(int) AS int, AVG(int) AS int, AVG(int) AS s FROM (SELECT MAX(int) AS int FROM test GROUP BY int) WHERE s > 0 "
        );
    }

    private String randomTopHitsFunction() {
        return randomFrom(Arrays.asList(First.class, Last.class)).getSimpleName().toUpperCase(Locale.ROOT);
    }
}
