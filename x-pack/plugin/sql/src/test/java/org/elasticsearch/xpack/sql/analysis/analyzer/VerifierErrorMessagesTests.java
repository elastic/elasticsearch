/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.TestUtils;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolverTests;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Coalesce;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Greatest;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfNull;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Least;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.NullIf;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.TypesTests;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class VerifierErrorMessagesTests extends ESTestCase {

    private SqlParser parser = new SqlParser();
    private IndexResolution indexResolution = IndexResolution.valid(new EsIndex("test",
        TypesTests.loadMapping("mapping-multi-field-with-nested.json")));

    private String error(String sql) {
        return error(indexResolution, sql);
    }

    private String error(IndexResolution getIndexResult, String sql) {
        Analyzer analyzer = new Analyzer(TestUtils.TEST_CFG, new FunctionRegistry(), getIndexResult, new Verifier(new Metrics()));
        VerificationException e = expectThrows(VerificationException.class, () -> analyzer.analyze(parser.createStatement(sql), true));
        assertTrue(e.getMessage().startsWith("Found "));
        String header = "Found 1 problem(s)\nline ";
        return e.getMessage().substring(header.length());
    }

    private LogicalPlan accept(String sql) {
        EsIndex test = getTestEsIndex();
        return accept(IndexResolution.valid(test), sql);
    }

    private EsIndex getTestEsIndex() {
        Map<String, EsField> mapping = TypesTests.loadMapping("mapping-multi-field-with-nested.json");
        return new EsIndex("test", mapping);
    }

    private LogicalPlan accept(IndexResolution resolution, String sql) {
        Analyzer analyzer = new Analyzer(TestUtils.TEST_CFG, new FunctionRegistry(), resolution, new Verifier(new Metrics()));
        return analyzer.analyze(parser.createStatement(sql), true);
    }

    private IndexResolution incompatible() {
        Map<String, EsField> basicMapping = TypesTests.loadMapping("mapping-basic.json", true);
        Map<String, EsField> incompatible = TypesTests.loadMapping("mapping-basic-incompatible.json");

        assertNotEquals(basicMapping, incompatible);
        IndexResolution resolution = IndexResolverTests.merge(new EsIndex("basic", basicMapping),
                new EsIndex("incompatible", incompatible));
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

        mapping.put("field", new EsField("field", DataType.OBJECT,
                singletonMap("alias", new EsField("alias", DataType.KEYWORD, emptyMap(), true)), false));

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
        assertEquals("1:14: Unknown column [a]\n" +
                "line 1:17: Unknown column [b]\n" +
                "line 1:22: Unknown column [c]\n" +
                "line 1:25: Unknown column [tex], did you mean [text]?", error("SELECT bool, a, b.*, c, tex.* FROM test"));
    }
    
    public void testMultipleColumnsWithWildcard2() {
        assertEquals("1:8: Unknown column [tex], did you mean [text]?\n" +
                "line 1:21: Unknown column [a]\n" +
                "line 1:24: Unknown column [dat], did you mean [date]?\n" +
                "line 1:31: Unknown column [c]", error("SELECT tex.*, bool, a, dat.*, c FROM test"));
    }
    
    public void testMultipleColumnsWithWildcard3() {
        assertEquals("1:8: Unknown column [ate], did you mean [date]?\n" +
                "line 1:21: Unknown column [keyw], did you mean [keyword]?\n" +
                "line 1:29: Unknown column [da], did you mean [date]?" , error("SELECT ate.*, bool, keyw.*, da FROM test"));
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
        assertEquals("1:56: Invalid ordinal [3] specified in [ORDER BY 2, 3] (valid range is [1, 2])",
                error("SELECT bool, MIN(int) FROM test GROUP BY 1 ORDER BY 2, 3"));
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
        assertEquals("1:8: Unknown datetime field [DOP], did you mean any of [DOM, DOW, DOY, IDOW]?",
            error("SELECT EXTRACT(DOP FROM date) FROM test"));
    }

    public void testExtractNonDateTime() {
        assertEquals("1:8: Invalid datetime field [ABS]. Use any datetime function.", error("SELECT EXTRACT(ABS FROM date) FROM test"));
    }

    public void testValidDateTimeFunctionsOnTime() {
        accept("SELECT HOUR_OF_DAY(CAST(date AS TIME)) FROM test");
        accept("SELECT MINUTE_OF_HOUR(CAST(date AS TIME)) FROM test");
        accept("SELECT MINUTE_OF_DAY(CAST(date AS TIME)) FROM test");
        accept("SELECT SECOND_OF_MINUTE(CAST(date AS TIME)) FROM test");
    }

    public void testInvalidDateTimeFunctionsOnTime() {
        assertEquals("1:8: argument of [DAY_OF_YEAR(CAST(date AS TIME))] must be [date or datetime], " +
                "found value [CAST(date AS TIME)] type [time]",
            error("SELECT DAY_OF_YEAR(CAST(date AS TIME)) FROM test"));
    }

    public void testGroupByOnTimeNotAllowed() {
        assertEquals("1:36: Function [CAST(date AS TIME)] with data type [time] cannot be used for grouping",
            error("SELECT count(*) FROM test GROUP BY CAST(date AS TIME)"));
    }

    public void testGroupByOnTimeWrappedWithScalar() {
        accept("SELECT count(*) FROM test GROUP BY MINUTE(CAST(date AS TIME))");
    }

    public void testHistogramOnTimeNotAllowed() {
        assertEquals("1:8: first argument of [HISTOGRAM] must be [date, datetime or numeric], " +
                "found value [CAST(date AS TIME)] type [time]",
            error("SELECT HISTOGRAM(CAST(date AS TIME), INTERVAL 1 MONTH), COUNT(*) FROM test GROUP BY 1"));
    }

    public void testSubtractFromInterval() {
        assertEquals("1:8: Cannot subtract a datetime[CAST('2000-01-01' AS DATETIME)] " +
                "from an interval[INTERVAL 1 MONTH]; do you mean the reverse?",
            error("SELECT INTERVAL 1 MONTH - CAST('2000-01-01' AS DATETIME)"));

        assertEquals("1:8: Cannot subtract a time[CAST('12:23:56.789' AS TIME)] " +
                "from an interval[INTERVAL 1 MONTH]; do you mean the reverse?",
            error("SELECT INTERVAL 1 MONTH - CAST('12:23:56.789' AS TIME)"));
    }

    public void testAddIntervalAndNumberNotAllowed() {
        assertEquals("1:8: [+] has arguments with incompatible types [INTERVAL_DAY] and [INTEGER]",
            error("SELECT INTERVAL 1 DAY + 100"));
        assertEquals("1:8: [+] has arguments with incompatible types [INTEGER] and [INTERVAL_DAY]",
            error("SELECT 100 + INTERVAL 1 DAY"));
    }

    public void testSubtractIntervalAndNumberNotAllowed() {
        assertEquals("1:8: [-] has arguments with incompatible types [INTERVAL_MINUTE] and [DOUBLE]",
            error("SELECT INTERVAL 10 MINUTE - 100.0"));
        assertEquals("1:8: [-] has arguments with incompatible types [DOUBLE] and [INTERVAL_MINUTE]",
            error("SELECT 100.0 - INTERVAL 10 MINUTE"));
    }

    public void testMultiplyIntervalWithDecimalNotAllowed() {
        assertEquals("1:8: [*] has arguments with incompatible types [INTERVAL_MONTH] and [DOUBLE]",
            error("SELECT INTERVAL 1 MONTH * 1.234"));
        assertEquals("1:8: [*] has arguments with incompatible types [DOUBLE] and [INTERVAL_MONTH]",
            error("SELECT 1.234 * INTERVAL 1 MONTH"));
    }

    public void testMultipleColumns() {
        assertEquals("1:43: Unknown column [xxx]\nline 1:8: Unknown column [xxx]",
                error("SELECT xxx FROM test GROUP BY DAY_oF_YEAR(xxx)"));
    }

    // GROUP BY
    public void testGroupBySelectWithAlias() {
        assertNotNull(accept("SELECT int AS i FROM test GROUP BY i"));
    }

    public void testGroupBySelectWithAliasOrderOnActualField() {
        assertNotNull(accept("SELECT int AS i FROM test GROUP BY i ORDER BY int"));
    }

    public void testGroupBySelectNonGrouped() {
        assertEquals("1:8: Cannot use non-grouped column [text], expected [int]",
                error("SELECT text, int FROM test GROUP BY int"));
    }

    public void testGroupByFunctionSelectFieldFromGroupByFunction() {
        assertEquals("1:8: Cannot use non-grouped column [int], expected [ABS(int)]",
                error("SELECT int FROM test GROUP BY ABS(int)"));
    }

    public void testGroupByOrderByNonGrouped() {
        assertEquals("1:50: Cannot order by non-grouped column [bool], expected [text]",
                error("SELECT MAX(int) FROM test GROUP BY text ORDER BY bool"));
    }

    public void testGroupByOrderByNonGrouped_WithHaving() {
        assertEquals("1:71: Cannot order by non-grouped column [bool], expected [text]",
            error("SELECT MAX(int) FROM test GROUP BY text HAVING MAX(int) > 10 ORDER BY bool"));
    }

    public void testGroupByOrdinalPointingToAggregate() {
        assertEquals("1:42: Ordinal [2] in [GROUP BY 2] refers to an invalid argument, aggregate function [MIN(int)]",
                error("SELECT bool, MIN(int) FROM test GROUP BY 2"));
    }

    public void testGroupByInvalidOrdinal() {
        assertEquals("1:42: Invalid ordinal [3] specified in [GROUP BY 3] (valid range is [1, 2])",
                error("SELECT bool, MIN(int) FROM test GROUP BY 3"));
    }

    public void testGroupByNegativeOrdinal() {
        assertEquals("1:42: Invalid ordinal [-1] specified in [GROUP BY -1] (valid range is [1, 2])",
                error("SELECT bool, MIN(int) FROM test GROUP BY -1"));
    }

    public void testGroupByOrderByAliasedInSelectAllowed() {
        LogicalPlan lp = accept("SELECT int i FROM test GROUP BY int ORDER BY i");
        assertNotNull(lp);
    }

    public void testGroupByOrderByScalarOverNonGrouped() {
        assertEquals("1:50: Cannot order by non-grouped column [YEAR(date)], expected [text] or an aggregate function",
                error("SELECT MAX(int) FROM test GROUP BY text ORDER BY YEAR(date)"));
    }

    public void testGroupByOrderByFieldFromGroupByFunction() {
        assertEquals("1:54: Cannot use non-grouped column [int], expected [ABS(int)]",
                error("SELECT ABS(int) FROM test GROUP BY ABS(int) ORDER BY int"));
    }

    public void testGroupByOrderByScalarOverNonGrouped_WithHaving() {
        assertEquals("1:71: Cannot order by non-grouped column [YEAR(date)], expected [text] or an aggregate function",
            error("SELECT MAX(int) FROM test GROUP BY text HAVING MAX(int) > 10 ORDER BY YEAR(date)"));
    }

    public void testGroupByHavingNonGrouped() {
        assertEquals("1:48: Cannot use HAVING filter on non-aggregate [int]; use WHERE instead",
                error("SELECT AVG(int) FROM test GROUP BY text HAVING int > 10"));
    }

    public void testGroupByAggregate() {
        assertEquals("1:36: Cannot use an aggregate [AVG] for grouping",
                error("SELECT AVG(int) FROM test GROUP BY AVG(int)"));
    }

    public void testStarOnNested() {
        assertNotNull(accept("SELECT dep.* FROM test"));
    }

    public void testGroupByOnInexact() {
        assertEquals("1:36: Field [text] of data type [text] cannot be used for grouping; " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT COUNT(*) FROM test GROUP BY text"));
    }

    public void testGroupByOnNested() {
        assertEquals("1:38: Grouping isn't (yet) compatible with nested fields [dep.dep_id]",
                error("SELECT dep.dep_id FROM test GROUP BY dep.dep_id"));
    }

    public void testHavingOnNested() {
        assertEquals("1:51: HAVING isn't (yet) compatible with nested fields [dep.start_date]",
                error("SELECT int FROM test GROUP BY int HAVING AVG(YEAR(dep.start_date)) > 1980"));
    }

    public void testGroupByScalarFunctionWithAggOnTarget() {
        assertEquals("1:31: Cannot use an aggregate [AVG] for grouping",
                error("SELECT int FROM test GROUP BY AVG(int) + 2"));
    }

    public void testUnsupportedType() {
        assertEquals("1:8: Cannot use field [unsupported] type [ip_range] as is unsupported",
                error("SELECT unsupported FROM test"));
    }

    public void testUnsupportedStarExpansion() {
        assertEquals("1:8: Cannot use field [unsupported] type [ip_range] as is unsupported",
                error("SELECT unsupported.* FROM test"));
    }

    public void testUnsupportedTypeInFilter() {
        assertEquals("1:26: Cannot use field [unsupported] type [ip_range] as is unsupported",
                error("SELECT * FROM test WHERE unsupported > 1"));
    }

    public void testTermEqualitOnInexact() {
        assertEquals("1:26: [text = 'value'] cannot operate on first argument field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT * FROM test WHERE text = 'value'"));
    }

    public void testTermEqualityOnAmbiguous() {
        assertEquals("1:26: [some.ambiguous = 'value'] cannot operate on first argument field of data type [text]: " +
                "Multiple exact keyword candidates available for [ambiguous]; specify which one to use",
            error("SELECT * FROM test WHERE some.ambiguous = 'value'"));
    }

    public void testUnsupportedTypeInFunction() {
        assertEquals("1:12: Cannot use field [unsupported] type [ip_range] as is unsupported",
                error("SELECT ABS(unsupported) FROM test"));
    }

    public void testUnsupportedTypeInOrder() {
        assertEquals("1:29: Cannot use field [unsupported] type [ip_range] as is unsupported",
                error("SELECT * FROM test ORDER BY unsupported"));
    }

    public void testInexactFieldInOrder() {
        assertEquals("1:29: ORDER BY cannot be applied to field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT * FROM test ORDER BY text"));
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
        assertEquals("1:44: Cannot order by non-grouped column [bool], expected [int]",
                error("SELECT int FROM test GROUP BY int ORDER BY bool"));
    }

    public void testGroupByOrderByScore() {
        assertEquals("1:44: Cannot order by non-grouped column [SCORE()], expected [int] or an aggregate function",
                error("SELECT int FROM test GROUP BY int ORDER BY SCORE()"));
    }

    public void testHavingOnColumn() {
        assertEquals("1:42: Cannot use HAVING filter on non-aggregate [int]; use WHERE instead",
                error("SELECT int FROM test GROUP BY int HAVING int > 2"));
    }

    public void testHavingOnScalar() {
        assertEquals("1:42: Cannot use HAVING filter on non-aggregate [int]; use WHERE instead",
                error("SELECT int FROM test GROUP BY int HAVING 2 < ABS(int)"));
    }

    public void testInWithDifferentDataTypes() {
        assertEquals("1:8: 2nd argument of [1 IN (2, '3', 4)] must be [integer], found value ['3'] type [keyword]",
            error("SELECT 1 IN (2, '3', 4)"));
    }

    public void testInWithDifferentDataTypesFromLeftValue() {
        assertEquals("1:8: 1st argument of [1 IN ('foo', 'bar')] must be [integer], found value ['foo'] type [keyword]",
            error("SELECT 1 IN ('foo', 'bar')"));
    }

    public void testInWithFieldInListOfValues() {
        assertEquals("1:26: Comparisons against variables are not (currently) supported; offender [int] in [int IN (1, int)]",
            error("SELECT * FROM test WHERE int IN (1, int)"));
    }

    public void testInOnFieldTextWithNoKeyword() {
        assertEquals("1:26: [IN] cannot operate on field of data type [text]: " +
            "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT * FROM test WHERE text IN ('foo', 'bar')"));
    }

    public void testNotSupportedAggregateOnDate() {
        assertEquals("1:8: argument of [AVG(date)] must be [numeric], found value [date] type [datetime]",
            error("SELECT AVG(date) FROM test"));
    }

    public void testInvalidTypeForStringFunction_WithOneArgString() {
        assertEquals("1:8: argument of [LENGTH(1)] must be [string], found value [1] type [integer]",
            error("SELECT LENGTH(1)"));
    }

    public void testInvalidTypeForStringFunction_WithOneArgNumeric() {
        assertEquals("1:8: argument of [CHAR('foo')] must be [integer], found value ['foo'] type [keyword]",
            error("SELECT CHAR('foo')"));
    }

    public void testInvalidTypeForNestedStringFunctions_WithOneArg() {
        assertEquals("1:14: argument of [CHAR('foo')] must be [integer], found value ['foo'] type [keyword]",
            error("SELECT ASCII(CHAR('foo'))"));
    }

    public void testInvalidTypeForNumericFunction_WithOneArg() {
        assertEquals("1:8: argument of [COS('foo')] must be [numeric], found value ['foo'] type [keyword]",
            error("SELECT COS('foo')"));
    }

    public void testInvalidTypeForBooleanFunction_WithOneArg() {
        assertEquals("1:8: argument of [NOT 'foo'] must be [boolean], found value ['foo'] type [keyword]",
            error("SELECT NOT 'foo'"));
    }

    public void testInvalidTypeForStringFunction_WithTwoArgs() {
        assertEquals("1:8: first argument of [CONCAT] must be [string], found value [1] type [integer]",
            error("SELECT CONCAT(1, 'bar')"));
        assertEquals("1:8: second argument of [CONCAT] must be [string], found value [2] type [integer]",
            error("SELECT CONCAT('foo', 2)"));
    }

    public void testInvalidTypeForNumericFunction_WithTwoArgs() {
        assertEquals("1:8: first argument of [TRUNCATE('foo', 2)] must be [numeric], found value ['foo'] type [keyword]",
            error("SELECT TRUNCATE('foo', 2)"));
        assertEquals("1:8: second argument of [TRUNCATE(1.2, 'bar')] must be [integer], found value ['bar'] type [keyword]",
            error("SELECT TRUNCATE(1.2, 'bar')"));
    }

    public void testInvalidTypeForBooleanFuntion_WithTwoArgs() {
        assertEquals("1:8: first argument of [1 OR true] must be [boolean], found value [1] type [integer]",
            error("SELECT 1 OR true"));
        assertEquals("1:8: second argument of [true OR 2] must be [boolean], found value [2] type [integer]",
            error("SELECT true OR 2"));
    }

    public void testInvalidTypeForReplace() {
        assertEquals("1:8: first argument of [REPLACE(1, 'foo', 'bar')] must be [string], found value [1] type [integer]",
            error("SELECT REPLACE(1, 'foo', 'bar')"));
        assertEquals("1:8: [REPLACE(text, 'foo', 'bar')] cannot operate on first argument field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT REPLACE(text, 'foo', 'bar') FROM test"));

        assertEquals("1:8: second argument of [REPLACE('foo', 2, 'bar')] must be [string], found value [2] type [integer]",
            error("SELECT REPLACE('foo', 2, 'bar')"));
        assertEquals("1:8: [REPLACE('foo', text, 'bar')] cannot operate on second argument field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT REPLACE('foo', text, 'bar') FROM test"));

        assertEquals("1:8: third argument of [REPLACE('foo', 'bar', 3)] must be [string], found value [3] type [integer]",
            error("SELECT REPLACE('foo', 'bar', 3)"));
        assertEquals("1:8: [REPLACE('foo', 'bar', text)] cannot operate on third argument field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT REPLACE('foo', 'bar', text) FROM test"));
    }

    public void testInvalidTypeForSubString() {
        assertEquals("1:8: first argument of [SUBSTRING(1, 2, 3)] must be [string], found value [1] type [integer]",
            error("SELECT SUBSTRING(1, 2, 3)"));
        assertEquals("1:8: [SUBSTRING(text, 2, 3)] cannot operate on first argument field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT SUBSTRING(text, 2, 3) FROM test"));

        assertEquals("1:8: second argument of [SUBSTRING('foo', 'bar', 3)] must be [integer], found value ['bar'] type [keyword]",
            error("SELECT SUBSTRING('foo', 'bar', 3)"));

        assertEquals("1:8: third argument of [SUBSTRING('foo', 2, 'bar')] must be [integer], found value ['bar'] type [keyword]",
            error("SELECT SUBSTRING('foo', 2, 'bar')"));
    }

    public void testInvalidTypeForFunction_WithFourArgs() {
        assertEquals("1:8: first argument of [INSERT(1, 1, 2, 'new')] must be [string], found value [1] type [integer]",
            error("SELECT INSERT(1, 1, 2, 'new')"));
        assertEquals("1:8: second argument of [INSERT('text', 'foo', 2, 'new')] must be [numeric], found value ['foo'] type [keyword]",
            error("SELECT INSERT('text', 'foo', 2, 'new')"));
        assertEquals("1:8: third argument of [INSERT('text', 1, 'bar', 'new')] must be [numeric], found value ['bar'] type [keyword]",
            error("SELECT INSERT('text', 1, 'bar', 'new')"));
        assertEquals("1:8: fourth argument of [INSERT('text', 1, 2, 3)] must be [string], found value [3] type [integer]",
            error("SELECT INSERT('text', 1, 2, 3)"));
    }

    public void testInvalidTypeForLikeMatch() {
        assertEquals("1:26: [text LIKE 'foo'] cannot operate on field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT * FROM test WHERE text LIKE 'foo'"));
    }
    
    public void testInvalidTypeForRLikeMatch() {
        assertEquals("1:26: [text RLIKE 'foo'] cannot operate on field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT * FROM test WHERE text RLIKE 'foo'"));
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
                incompatibleError("SELECT emp_no FROM \"*\""));
    }

    public void testMismatchedFieldStarInIncompatibleMappings() {
        assertEquals(
                "1:8: Cannot use field [emp_no] due to ambiguities being mapped as [2] incompatible types: "
                        + "[integer] in [basic], [long] in [incompatible]",
                incompatibleError("SELECT emp_no.* FROM \"*\""));
    }

    public void testMismatchedFieldFilterInIncompatibleMappings() {
        assertEquals(
                "1:33: Cannot use field [emp_no] due to ambiguities being mapped as [2] incompatible types: "
                        + "[integer] in [basic], [long] in [incompatible]",
                incompatibleError("SELECT languages FROM \"*\" WHERE emp_no > 1"));
    }

    public void testMismatchedFieldScalarInIncompatibleMappings() {
        assertEquals(
                "1:45: Cannot use field [emp_no] due to ambiguities being mapped as [2] incompatible types: "
                        + "[integer] in [basic], [long] in [incompatible]",
                incompatibleError("SELECT languages FROM \"*\" ORDER BY SIGN(ABS(emp_no))"));
    }

    public void testConditionalWithDifferentDataTypes() {
        @SuppressWarnings("unchecked")
        String function = randomFrom(IfNull.class, NullIf.class).getSimpleName();
        assertEquals("1:17: 2nd argument of [" + function + "(3, '4')] must be [integer], found value ['4'] type [keyword]",
            error("SELECT 1 = 1 OR " + function + "(3, '4') > 1"));

        @SuppressWarnings("unchecked")
        String arbirtraryArgsFunction = randomFrom(Coalesce.class, Greatest.class, Least.class).getSimpleName();
        assertEquals("1:17: 3rd argument of [" + arbirtraryArgsFunction + "(null, 3, '4')] must be [integer], " +
                "found value ['4'] type [keyword]",
            error("SELECT 1 = 1 OR " + arbirtraryArgsFunction + "(null, 3, '4') > 1"));
    }

    public void testCaseWithNonBooleanConditionExpression() {
        assertEquals("1:8: condition of [WHEN abs(int) THEN 'foo'] must be [boolean], found value [abs(int)] type [integer]",
            error("SELECT CASE WHEN int = 1 THEN 'one' WHEN abs(int) THEN 'foo' END FROM test"));
    }

    public void testCaseWithDifferentResultDataTypes() {
        assertEquals("1:8: result of [WHEN int > 10 THEN 10] must be [keyword], found value [10] type [integer]",
            error("SELECT CASE WHEN int > 20 THEN 'foo' WHEN int > 10 THEN 10 ELSE 'bar' END FROM test"));
    }

    public void testCaseWithDifferentResultAndDefaultValueDataTypes() {
        assertEquals("1:8: ELSE clause of [date] must be [keyword], found value [date] type [datetime]",
            error("SELECT CASE WHEN int > 20 THEN 'foo' ELSE date END FROM test"));
    }

    public void testCaseWithDifferentResultAndDefaultValueDataTypesAndNullTypesSkipped() {
        assertEquals("1:8: ELSE clause of [date] must be [keyword], found value [date] type [datetime]",
            error("SELECT CASE WHEN int > 20 THEN null WHEN int > 10 THEN null WHEN int > 5 THEN 'foo' ELSE date END FROM test"));
    }

    public void testIifWithNonBooleanConditionExpression() {
        assertEquals("1:8: first argument of [IIF(int, 'one', 'zero')] must be [boolean], found value [int] type [integer]",
            error("SELECT IIF(int, 'one', 'zero') FROM test"));
    }

    public void testIifWithDifferentResultAndDefaultValueDataTypes() {
        assertEquals("1:8: third argument of [IIF(int > 20, 'foo', date)] must be [keyword], found value [date] type [datetime]",
            error("SELECT IIF(int > 20, 'foo', date) FROM test"));
    }

    public void testAggsInWhere() {
        assertEquals("1:33: Cannot use WHERE filtering on aggregate function [MAX(int)], use HAVING instead",
                error("SELECT MAX(int) FROM test WHERE MAX(int) > 10 GROUP BY bool"));
    }

    public void testHistogramInFilter() {
        assertEquals("1:63: Cannot filter on grouping function [HISTOGRAM(date, INTERVAL 1 MONTH)], use its argument instead",
                error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) AS h FROM test WHERE "
                        + "HISTOGRAM(date, INTERVAL 1 MONTH) > CAST('2000-01-01' AS DATETIME) GROUP BY h"));
    }

    // related https://github.com/elastic/elasticsearch/issues/36853
    public void testHistogramInHaving() {
        assertEquals("1:75: Cannot filter on grouping function [h], use its argument instead",
                error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) AS h FROM test GROUP BY h HAVING "
                        + "h > CAST('2000-01-01' AS DATETIME)"));
    }

    public void testGroupByScalarOnTopOfGrouping() {
        assertEquals(
                "1:14: Cannot combine [HISTOGRAM(date, INTERVAL 1 MONTH)] grouping function inside "
                        + "GROUP BY, found [MONTH(HISTOGRAM(date, INTERVAL 1 MONTH))]; consider moving the expression inside the histogram",
                error("SELECT MONTH(HISTOGRAM(date, INTERVAL 1 MONTH)) AS h FROM test GROUP BY h"));
    }

    public void testAggsInHistogram() {
        assertEquals("1:37: Cannot use an aggregate [MAX] for grouping",
                error("SELECT MAX(date) FROM test GROUP BY MAX(int)"));
    }

    public void testGroupingsInHistogram() {
        assertEquals(
                "1:47: Cannot embed grouping functions within each other, found [HISTOGRAM(int, 1)] in [HISTOGRAM(HISTOGRAM(int, 1), 1)]",
                error("SELECT MAX(date) FROM test GROUP BY HISTOGRAM(HISTOGRAM(int, 1), 1)"));
    }

    public void testCastInHistogram() {
        accept("SELECT MAX(date) FROM test GROUP BY HISTOGRAM(CAST(int AS LONG), 1)");
    }

    public void testHistogramNotInGrouping() {
        assertEquals("1:8: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
                error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) AS h FROM test"));
    }
    
    public void testHistogramNotInGroupingWithCount() {
        assertEquals("1:8: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
                error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) AS h, COUNT(*) FROM test"));
    }
    
    public void testHistogramNotInGroupingWithMaxFirst() {
        assertEquals("1:19: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
                error("SELECT MAX(date), HISTOGRAM(date, INTERVAL 1 MONTH) AS h FROM test"));
    }
    
    public void testHistogramWithoutAliasNotInGrouping() {
        assertEquals("1:8: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
                error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) FROM test"));
    }
    
    public void testTwoHistogramsNotInGrouping() {
        assertEquals("1:48: [HISTOGRAM(date, INTERVAL 1 DAY)] needs to be part of the grouping",
                error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) AS h, HISTOGRAM(date, INTERVAL 1 DAY) FROM test GROUP BY h"));
    }
    
    public void testHistogramNotInGrouping_WithGroupByField() {
        assertEquals("1:8: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
                error("SELECT HISTOGRAM(date, INTERVAL 1 MONTH) FROM test GROUP BY date"));
    }
    
    public void testScalarOfHistogramNotInGrouping() {
        assertEquals("1:14: [HISTOGRAM(date, INTERVAL 1 MONTH)] needs to be part of the grouping",
                error("SELECT MONTH(HISTOGRAM(date, INTERVAL 1 MONTH)) FROM test"));
    }

    public void testErrorMessageForPercentileWithSecondArgBasedOnAField() {
        assertEquals("1:8: second argument of [PERCENTILE(int, ABS(int))] must be a constant, received [ABS(int)]",
            error("SELECT PERCENTILE(int, ABS(int)) FROM test"));
    }

    public void testErrorMessageForPercentileRankWithSecondArgBasedOnAField() {
        assertEquals("1:8: second argument of [PERCENTILE_RANK(int, ABS(int))] must be a constant, received [ABS(int)]",
            error("SELECT PERCENTILE_RANK(int, ABS(int)) FROM test"));
    }

    public void testTopHitsFirstArgConstant() {
        assertEquals("1:8: first argument of [FIRST('foo', int)] must be a table column, found constant ['foo']",
            error("SELECT FIRST('foo', int) FROM test"));
    }

    public void testTopHitsSecondArgConstant() {
        assertEquals("1:8: second argument of [LAST(int, 10)] must be a table column, found constant [10]",
            error("SELECT LAST(int, 10) FROM test"));
    }

    public void testTopHitsFirstArgTextWithNoKeyword() {
        assertEquals("1:8: [FIRST(text)] cannot operate on first argument field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT FIRST(text) FROM test"));
    }

    public void testTopHitsSecondArgTextWithNoKeyword() {
        assertEquals("1:8: [LAST(keyword, text)] cannot operate on second argument field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT LAST(keyword, text) FROM test"));
    }

    public void testTopHitsGroupByHavingUnsupported() {
        assertEquals("1:50: HAVING filter is unsupported for function [FIRST(int)]",
            error("SELECT FIRST(int) FROM test GROUP BY text HAVING FIRST(int) > 10"));
    }

    public void testMinOnInexactUnsupported() {
        assertEquals("1:8: [MIN(text)] cannot operate on field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT MIN(text) FROM test"));
    }

    public void testMaxOnInexactUnsupported() {
        assertEquals("1:8: [MAX(text)] cannot operate on field of data type [text]: " +
                "No keyword/multi-field defined exact matches for [text]; define one or use MATCH/QUERY instead",
            error("SELECT MAX(text) FROM test"));
    }

    public void testMinOnKeywordGroupByHavingUnsupported() {
        assertEquals("1:52: HAVING filter is unsupported for function [MIN(keyword)]",
            error("SELECT MIN(keyword) FROM test GROUP BY text HAVING MIN(keyword) > 10"));
    }

    public void testMaxOnKeywordGroupByHavingUnsupported() {
        assertEquals("1:52: HAVING filter is unsupported for function [MAX(keyword)]",
            error("SELECT MAX(keyword) FROM test GROUP BY text HAVING MAX(keyword) > 10"));
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

    public void testGeoShapeInWhereClause() {
        assertEquals("1:49: geo shapes cannot be used for filtering",
            error("SELECT ST_AsWKT(shape) FROM test WHERE ST_AsWKT(shape) = 'point (10 20)'"));

        // We get only one message back because the messages are grouped by the node that caused the issue
        assertEquals("1:46: geo shapes cannot be used for filtering",
            error("SELECT MAX(ST_X(shape)) FROM test WHERE ST_Y(shape) > 10 GROUP BY ST_GEOMETRYTYPE(shape) ORDER BY ST_ASWKT(shape)"));
    }

    public void testGeoShapeInGroupBy() {
        assertEquals("1:44: geo shapes cannot be used in grouping",
            error("SELECT ST_X(shape) FROM test GROUP BY ST_X(shape)"));
    }

    public void testGeoShapeInOrderBy() {
        assertEquals("1:44: geo shapes cannot be used for sorting",
            error("SELECT ST_X(shape) FROM test ORDER BY ST_Z(shape)"));
    }

    public void testGeoShapeInSelect() {
        accept("SELECT ST_X(shape) FROM test");
    }

}
