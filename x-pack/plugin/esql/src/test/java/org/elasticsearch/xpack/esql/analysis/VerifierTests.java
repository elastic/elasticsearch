/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.Build;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesRegex;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class VerifierTests extends ESTestCase {

    private static final EsqlParser parser = new EsqlParser();
    private final Analyzer defaultAnalyzer = AnalyzerTestUtils.expandedDefaultAnalyzer();
    private final Analyzer tsdb = AnalyzerTestUtils.analyzer(AnalyzerTestUtils.tsdbIndexResolution());

    public void testIncompatibleTypesInMathOperation() {
        assertEquals(
            "1:40: second argument of [a + c] must be [datetime or numeric], found value [c] type [keyword]",
            error("row a = 1, b = 2, c = \"xxx\" | eval y = a + c")
        );
        assertEquals(
            "1:40: second argument of [a - c] must be [datetime or numeric], found value [c] type [keyword]",
            error("row a = 1, b = 2, c = \"xxx\" | eval y = a - c")
        );
    }

    public void testRoundFunctionInvalidInputs() {
        assertEquals(
            "1:31: first argument of [round(b, 3)] must be [numeric], found value [b] type [keyword]",
            error("row a = 1, b = \"c\" | eval x = round(b, 3)")
        );
        assertEquals(
            "1:31: first argument of [round(b)] must be [numeric], found value [b] type [keyword]",
            error("row a = 1, b = \"c\" | eval x = round(b)")
        );
        assertEquals(
            "1:31: second argument of [round(a, b)] must be [integer], found value [b] type [keyword]",
            error("row a = 1, b = \"c\" | eval x = round(a, b)")
        );
        assertEquals(
            "1:31: second argument of [round(a, 3.5)] must be [integer], found value [3.5] type [double]",
            error("row a = 1, b = \"c\" | eval x = round(a, 3.5)")
        );
        assertEquals(
            "1:23: Cannot convert string [c] to [INTEGER], error [Cannot parse number [c]]",
            error("row a = round(123.45, \"c\")")
        );
    }

    public void testAggsExpressionsInStatsAggs() {
        assertEquals(
            "1:44: column [salary] must appear in the STATS BY clause or be used in an aggregate function",
            error("from test | eval z = 2 | stats x = avg(z), salary by emp_no")
        );
        assertEquals(
            "1:23: nested aggregations [max(salary)] not allowed inside other aggregations [max(max(salary))]",
            error("from test | stats max(max(salary)) by first_name")
        );
        assertEquals(
            "1:25: argument of [avg(first_name)] must be [numeric except unsigned_long or counter types],"
                + " found value [first_name] type [keyword]",
            error("from test | stats count(avg(first_name)) by first_name")
        );
        assertEquals(
            "1:23: second argument of [percentile(languages, languages)] must be a constant, received [languages]",
            error("from test | stats x = percentile(languages, languages) by emp_no")
        );
        assertEquals(
            "1:23: second argument of [count_distinct(languages, languages)] must be a constant, received [languages]",
            error("from test | stats x = count_distinct(languages, languages) by emp_no")
        );
        // no agg function
        assertEquals("1:19: expected an aggregate function but found [5]", error("from test | stats 5 by emp_no"));

        // don't allow naked group
        assertEquals("1:19: grouping key [emp_no] already specified in the STATS BY clause", error("from test | stats emp_no BY emp_no"));
        // don't allow naked group - even when it's an expression
        assertEquals(
            "1:19: grouping key [languages + emp_no] already specified in the STATS BY clause",
            error("from test | stats languages + emp_no BY languages + emp_no")
        );
        // don't allow group alias
        assertEquals(
            "1:19: grouping key [e] already specified in the STATS BY clause",
            error("from test | stats e BY e = languages + emp_no")
        );

        var message = error("from test | stats languages + emp_no BY e = languages + emp_no");
        assertThat(
            message,
            containsString(
                "column [emp_no] cannot be used as an aggregate once declared in the STATS BY grouping key [e = languages + emp_no]"
            )
        );
        assertThat(
            message,
            containsString(
                " column [languages] cannot be used as an aggregate once declared in the STATS BY grouping key [e = languages + emp_no]"
            )
        );
    }

    public void testAggsInsideGrouping() {
        assertEquals(
            "1:36: cannot use an aggregate [max(languages)] for grouping",
            error("from test| stats max(languages) by max(languages)")
        );
    }

    public void testGroupingInsideAggsAsAgg() {
        assertEquals(
            "1:18: can only use grouping function [bucket(emp_no, 5.)] part of the BY clause",
            error("from test| stats bucket(emp_no, 5.) by emp_no")
        );
        assertEquals(
            "1:18: can only use grouping function [bucket(emp_no, 5.)] part of the BY clause",
            error("from test| stats bucket(emp_no, 5.)")
        );
        assertEquals(
            "1:18: can only use grouping function [bucket(emp_no, 5.)] part of the BY clause",
            error("from test| stats bucket(emp_no, 5.) by bucket(emp_no, 6.)")
        );
        assertEquals(
            "1:22: can only use grouping function [bucket(emp_no, 5.)] part of the BY clause",
            error("from test| stats 3 + bucket(emp_no, 5.) by bucket(emp_no, 6.)")
        );
    }

    public void testGroupingInsideAggsAsGrouping() {
        assertEquals(
            "1:18: grouping function [bucket(emp_no, 5.)] cannot be used as an aggregate once declared in the STATS BY clause",
            error("from test| stats bucket(emp_no, 5.) by bucket(emp_no, 5.)")
        );
        assertEquals(
            "1:18: grouping function [bucket(emp_no, 5.)] cannot be used as an aggregate once declared in the STATS BY clause",
            error("from test| stats bucket(emp_no, 5.) by emp_no, bucket(emp_no, 5.)")
        );
        assertEquals(
            "1:18: grouping function [bucket(emp_no, 5.)] cannot be used as an aggregate once declared in the STATS BY clause",
            error("from test| stats bucket(emp_no, 5.) by x = bucket(emp_no, 5.)")
        );
        assertEquals(
            "1:22: grouping function [bucket(emp_no, 5.)] cannot be used as an aggregate once declared in the STATS BY clause",
            error("from test| stats z = bucket(emp_no, 5.) by x = bucket(emp_no, 5.)")
        );
        assertEquals(
            "1:22: grouping function [bucket(emp_no, 5.)] cannot be used as an aggregate once declared in the STATS BY clause",
            error("from test| stats y = bucket(emp_no, 5.) by y = bucket(emp_no, 5.)")
        );
        assertEquals(
            "1:22: grouping function [bucket(emp_no, 5.)] cannot be used as an aggregate once declared in the STATS BY clause",
            error("from test| stats z = bucket(emp_no, 5.) by bucket(emp_no, 5.)")
        );
    }

    public void testGroupingInsideGrouping() {
        assertEquals(
            "1:40: cannot nest grouping functions; found [bucket(emp_no, 5.)] inside [bucket(bucket(emp_no, 5.), 6.)]",
            error("from test| stats max(emp_no) by bucket(bucket(emp_no, 5.), 6.)")
        );
    }

    public void testAggsWithInvalidGrouping() {
        assertEquals(
            "1:35: column [languages] cannot be used as an aggregate once declared in the STATS BY grouping key [l = languages % 3]",
            error("from test| stats max(languages) + languages by l = languages % 3")
        );
    }

    public void testGroupingAlias() throws Exception {
        assertEquals(
            "1:23: column [languages] cannot be used as an aggregate once declared in the STATS BY grouping key [l = languages % 3]",
            error("from test | stats l = languages + 3 by l = languages % 3 | keep l")
        );
    }

    public void testGroupingAliasDuplicate() throws Exception {
        assertEquals(
            "1:22: column [languages] cannot be used as an aggregate "
                + "once declared in the STATS BY grouping key [l = languages % 3, l = languages, l = languages % 2]",
            error("from test| stats l = languages + 3 by l = languages % 3, l = languages, l = languages % 2 | keep l")
        );

        assertEquals(
            "1:22: column [languages] cannot be used as an aggregate " + "once declared in the STATS BY grouping key [l = languages % 3]",
            error("from test| stats l = languages + 3, l = languages % 2  by l = languages % 3 | keep l")
        );

    }

    public void testAggsIgnoreCanonicalGrouping() {
        // the grouping column should appear verbatim - ignore canonical representation as they complicate things significantly
        // for no real benefit (1+languages != languages + 1)
        assertEquals(
            "1:39: column [languages] cannot be used as an aggregate once declared in the STATS BY grouping key [l = languages + 1]",
            error("from test| stats max(languages) + 1 + languages by l = languages + 1")
        );
    }

    public void testAggsWithoutAgg() {
        // should work
        assertEquals(
            "1:35: column [salary] must appear in the STATS BY clause or be used in an aggregate function",
            error("from test| stats max(languages) + salary by l = languages + 1")
        );
    }

    public void testAggsInsideEval() throws Exception {
        assertEquals("1:29: aggregate function [max(b)] not allowed outside STATS command", error("row a = 1, b = 2 | eval x = max(b)"));
    }

    public void testGroupingInAggs() {
        assertEquals("2:12: column [salary] must appear in the STATS BY clause or be used in an aggregate function", error("""
             from test
            |stats e = salary + max(salary) by languages
            """));
    }

    public void testBucketOnlyInAggs() {
        assertEquals(
            "1:23: cannot use grouping function [BUCKET(emp_no, 100.)] outside of a STATS command",
            error("FROM test | WHERE ABS(BUCKET(emp_no, 100.)) > 0")
        );
        assertEquals(
            "1:22: cannot use grouping function [BUCKET(emp_no, 100.)] outside of a STATS command",
            error("FROM test | EVAL 3 + BUCKET(emp_no, 100.)")
        );
        assertEquals(
            "1:18: cannot use grouping function [BUCKET(emp_no, 100.)] outside of a STATS command",
            error("FROM test | SORT BUCKET(emp_no, 100.)")
        );
    }

    public void testDoubleRenamingField() {
        assertEquals(
            "1:44: Column [emp_no] renamed to [r1] and is no longer available [emp_no as r3]",
            error("from test | rename emp_no as r1, r1 as r2, emp_no as r3 | keep r3")
        );
    }

    public void testDuplicateRenaming() {
        assertEquals(
            "1:34: Column [emp_no] renamed to [r1] and is no longer available [emp_no as r1]",
            error("from test | rename emp_no as r1, emp_no as r1 | keep r1")
        );
    }

    public void testDoubleRenamingReference() {
        assertEquals(
            "1:61: Column [r1] renamed to [r2] and is no longer available [r1 as r3]",
            error("from test | rename emp_no as r1, r1 as r2, first_name as x, r1 as r3 | keep r3")
        );
    }

    public void testDropAfterRenaming() {
        assertEquals("1:40: Unknown column [emp_no]", error("from test | rename emp_no as r1 | drop emp_no"));
    }

    public void testNonStringFieldsInDissect() {
        assertEquals(
            "1:21: Dissect only supports KEYWORD or TEXT values, found expression [emp_no] type [INTEGER]",
            error("from test | dissect emp_no \"%{foo}\"")
        );
    }

    public void testNonStringFieldsInGrok() {
        assertEquals(
            "1:18: Grok only supports KEYWORD or TEXT values, found expression [emp_no] type [INTEGER]",
            error("from test | grok emp_no \"%{WORD:foo}\"")
        );
    }

    public void testMixedNonConvertibleTypesInIn() {
        assertEquals(
            "1:19: 2nd argument of [emp_no in (1, \"two\")] must be [integer], found value [\"two\"] type [keyword]",
            error("from test | where emp_no in (1, \"two\")")
        );
    }

    public void testMixedNumericalNonConvertibleTypesInIn() {
        assertEquals(
            "1:19: 2nd argument of [3 in (1, to_ul(3))] must be [integer], found value [to_ul(3)] type [unsigned_long]",
            error("from test | where 3 in (1, to_ul(3))")
        );
        assertEquals(
            "1:19: 1st argument of [to_ul(3) in (1, 3)] must be [unsigned_long], found value [1] type [integer]",
            error("from test | where to_ul(3) in (1, 3)")
        );
    }

    public void testUnsignedLongTypeMixInComparisons() {
        List<String> types = DataType.types()
            .stream()
            .filter(dt -> dt.isNumeric() && DataType.isRepresentable(dt) && dt != UNSIGNED_LONG)
            .map(DataType::typeName)
            .toList();
        for (var type : types) {
            for (var comp : List.of("==", "!=", ">", ">=", "<=", "<")) {
                String left, right, leftType, rightType;
                if (randomBoolean()) {
                    left = "ul";
                    leftType = "unsigned_long";
                    right = "n";
                    rightType = type;
                } else {
                    left = "n";
                    leftType = type;
                    right = "ul";
                    rightType = "unsigned_long";
                }
                var operation = left + " " + comp + " " + right;
                assertThat(
                    error("row n = to_" + type + "(1), ul = to_ul(1) | where " + operation),
                    containsString(
                        "first argument of ["
                            + operation
                            + "] is ["
                            + leftType
                            + "] and second is ["
                            + rightType
                            + "]."
                            + " [unsigned_long] can only be operated on together with another [unsigned_long]"
                    )
                );
            }
        }
    }

    public void testUnsignedLongTypeMixInArithmetics() {
        List<String> types = DataType.types()
            .stream()
            .filter(dt -> dt.isNumeric() && DataType.isRepresentable(dt) && dt != UNSIGNED_LONG)
            .map(DataType::typeName)
            .toList();
        for (var type : types) {
            for (var operation : List.of("+", "-", "*", "/", "%")) {
                String left, right, leftType, rightType;
                if (randomBoolean()) {
                    left = "ul";
                    leftType = "unsigned_long";
                    right = "n";
                    rightType = type;
                } else {
                    left = "n";
                    leftType = type;
                    right = "ul";
                    rightType = "unsigned_long";
                }
                var op = left + " " + operation + " " + right;
                assertThat(
                    error("row n = to_" + type + "(1), ul = to_ul(1) | eval " + op),
                    containsString("[" + operation + "] has arguments with incompatible types [" + leftType + "] and [" + rightType + "]")
                );
            }
        }
    }

    public void testUnsignedLongNegation() {
        assertEquals(
            "1:29: argument of [-x] must be [numeric, date_period or time_duration], found value [x] type [unsigned_long]",
            error("row x = to_ul(1) | eval y = -x")
        );
    }

    public void testSumOnDate() {
        assertEquals(
            "1:19: argument of [sum(hire_date)] must be [numeric except unsigned_long or counter types],"
                + " found value [hire_date] type [datetime]",
            error("from test | stats sum(hire_date)")
        );
    }

    public void testWrongInputParam() {
        assertEquals(
            "1:19: first argument of [emp_no == ?] is [numeric] so second argument must also be [numeric] but was [keyword]",
            error("from test | where emp_no == ?", "foo")
        );

        assertEquals(
            "1:19: first argument of [emp_no == ?] is [numeric] so second argument must also be [numeric] but was [null]",
            error("from test | where emp_no == ?", new Object[] { null })
        );
    }

    public void testPeriodAndDurationInRowAssignment() {
        for (var unit : List.of("millisecond", "second", "minute", "hour", "day", "week", "month", "year")) {
            assertEquals("1:5: cannot use [1 " + unit + "] directly in a row assignment", error("row a = 1 " + unit));
        }
    }

    public void testSubtractDateTimeFromTemporal() {
        for (var unit : List.of("millisecond", "second", "minute", "hour")) {
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] from a [TIME_DURATION] amount [1 "
                    + unit
                    + "]",
                error("row 1 " + unit + " - now() ")
            );
        }
        for (var unit : List.of("day", "week", "month", "year")) {
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] from a [DATE_PERIOD] amount [1 "
                    + unit
                    + "]",
                error("row 1 " + unit + " - now() ")
            );
        }
    }

    public void testPeriodAndDurationInEval() {
        for (var unit : List.of("millisecond", "second", "minute", "hour")) {
            assertEquals(
                "1:18: EVAL does not support type [time_duration] in expression [1 " + unit + "]",
                error("row x = 1 | eval y = 1 " + unit)
            );
        }
        for (var unit : List.of("day", "week", "month", "year")) {
            assertEquals(
                "1:18: EVAL does not support type [date_period] in expression [1 " + unit + "]",
                error("row x = 1 | eval y = 1 " + unit)
            );
        }
    }

    public void testFilterNonBoolField() {
        assertEquals("1:19: Condition expression needs to be boolean, found [INTEGER]", error("from test | where emp_no"));
    }

    public void testFilterDateConstant() {
        assertEquals("1:19: Condition expression needs to be boolean, found [DATE_PERIOD]", error("from test | where 1 year"));
    }

    public void testNestedAggField() {
        assertEquals("1:27: Unknown column [avg]", error("from test | stats c = avg(avg)"));
    }

    public void testNotFoundFieldInNestedFunction() {
        assertEquals("""
            1:30: Unknown column [missing]
            line 1:43: Unknown column [not_found]
            line 1:23: Unknown column [avg]""", error("from test | stats c = avg by missing + 1, not_found"));
    }

    public void testSpatialSort() {
        String prefix = "ROW wkt = [\"POINT(42.9711 -14.7553)\", \"POINT(75.8093 22.7277)\"] | MV_EXPAND wkt ";
        assertEquals("1:130: cannot sort on geo_point", error(prefix + "| EVAL shape = TO_GEOPOINT(wkt) | limit 5 | sort shape"));
        assertEquals(
            "1:136: cannot sort on cartesian_point",
            error(prefix + "| EVAL shape = TO_CARTESIANPOINT(wkt) | limit 5 | sort shape")
        );
        assertEquals("1:130: cannot sort on geo_shape", error(prefix + "| EVAL shape = TO_GEOSHAPE(wkt) | limit 5 | sort shape"));
        assertEquals(
            "1:136: cannot sort on cartesian_shape",
            error(prefix + "| EVAL shape = TO_CARTESIANSHAPE(wkt) | limit 5 | sort shape")
        );
        var airports = AnalyzerTestUtils.analyzer(loadMapping("mapping-airports.json", "airports"));
        var airportsWeb = AnalyzerTestUtils.analyzer(loadMapping("mapping-airports_web.json", "airports_web"));
        var countriesBbox = AnalyzerTestUtils.analyzer(loadMapping("mapping-countries_bbox.json", "countries_bbox"));
        var countriesBboxWeb = AnalyzerTestUtils.analyzer(loadMapping("mapping-countries_bbox_web.json", "countries_bbox_web"));
        assertEquals("1:32: cannot sort on geo_point", error("FROM airports | LIMIT 5 | sort location", airports));
        assertEquals("1:36: cannot sort on cartesian_point", error("FROM airports_web | LIMIT 5 | sort location", airportsWeb));
        assertEquals("1:38: cannot sort on geo_shape", error("FROM countries_bbox | LIMIT 5 | sort shape", countriesBbox));
        assertEquals("1:42: cannot sort on cartesian_shape", error("FROM countries_bbox_web | LIMIT 5 | sort shape", countriesBboxWeb));
    }

    public void testInlineImpossibleConvert() {
        assertEquals("1:5: argument of [false::ip] must be [ip or string], found value [false] type [boolean]", error("ROW false::ip"));
    }

    public void testAggregateOnCounter() {
        assertThat(
            error("FROM tests | STATS min(network.bytes_in)", tsdb),
            equalTo(
                "1:20: argument of [min(network.bytes_in)] must be"
                    + " [boolean, datetime, ip or numeric except unsigned_long or counter types],"
                    + " found value [network.bytes_in] type [counter_long]"
            )
        );

        assertThat(
            error("FROM tests | STATS max(network.bytes_in)", tsdb),
            equalTo(
                "1:20: argument of [max(network.bytes_in)] must be"
                    + " [boolean, datetime, ip or numeric except unsigned_long or counter types],"
                    + " found value [network.bytes_in] type [counter_long]"
            )
        );

        assertThat(
            error("FROM tests | STATS count(network.bytes_out)", tsdb),
            equalTo(
                "1:20: argument of [count(network.bytes_out)] must be [any type except counter types],"
                    + " found value [network.bytes_out] type [counter_long]"
            )
        );
    }

    public void testGroupByCounter() {
        assertThat(
            error("FROM tests | STATS count(*) BY network.bytes_in", tsdb),
            equalTo("1:32: cannot group by on [counter_long] type for grouping [network.bytes_in]")
        );
    }

    public void testAggsResolutionWithUnresolvedGroupings() {
        String agg_func = randomFrom(
            new String[] { "avg", "count", "count_distinct", "min", "max", "median", "median_absolute_deviation", "sum", "values" }
        );

        assertThat(error("FROM tests | STATS " + agg_func + "(emp_no) by foobar"), matchesRegex("1:\\d+: Unknown column \\[foobar]"));
        assertThat(
            error("FROM tests | STATS " + agg_func + "(x) by foobar, x = emp_no"),
            matchesRegex("1:\\d+: Unknown column \\[foobar]")
        );
        assertThat(error("FROM tests | STATS " + agg_func + "(foobar) by foobar"), matchesRegex("1:\\d+: Unknown column \\[foobar]"));
        assertThat(
            error("FROM tests | STATS " + agg_func + "(foobar) by BUCKET(languages, 10)"),
            matchesRegex(
                "1:\\d+: function expects exactly four arguments when the first one is of type \\[INTEGER]"
                    + " and the second of type \\[INTEGER]\n"
                    + "line 1:\\d+: Unknown column \\[foobar]"
            )
        );
        assertThat(error("FROM tests | STATS " + agg_func + "(foobar) by emp_no"), matchesRegex("1:\\d+: Unknown column \\[foobar]"));
        // TODO: Ideally, we'd detect that count_distinct(x) doesn't require an error message.
        assertThat(
            error("FROM tests | STATS " + agg_func + "(x) by x = foobar"),
            matchesRegex("1:\\d+: Unknown column \\[foobar]\n" + "line 1:\\d+: Unknown column \\[x]")
        );
    }

    public void testNotAllowRateOutsideMetrics() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        assertThat(
            error("FROM tests | STATS avg(rate(network.bytes_in))", tsdb),
            equalTo("1:24: the rate aggregate[rate(network.bytes_in)] can only be used within the metrics command")
        );
        assertThat(
            error("METRICS tests | STATS sum(rate(network.bytes_in))", tsdb),
            equalTo("1:27: the rate aggregate[rate(network.bytes_in)] can only be used within the metrics command")
        );
        assertThat(
            error("FROM tests | STATS rate(network.bytes_in)", tsdb),
            equalTo("1:20: the rate aggregate[rate(network.bytes_in)] can only be used within the metrics command")
        );
        assertThat(
            error("FROM tests | EVAL r = rate(network.bytes_in)", tsdb),
            equalTo("1:23: aggregate function [rate(network.bytes_in)] not allowed outside METRICS command")
        );
    }

    public void testRateNotEnclosedInAggregate() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        assertThat(
            error("METRICS tests rate(network.bytes_in)", tsdb),
            equalTo(
                "1:15: the rate aggregate [rate(network.bytes_in)] can only be used within the metrics command and inside another aggregate"
            )
        );
        assertThat(
            error("METRICS tests avg(rate(network.bytes_in)), rate(network.bytes_in)", tsdb),
            equalTo(
                "1:44: the rate aggregate [rate(network.bytes_in)] can only be used within the metrics command and inside another aggregate"
            )
        );
        assertThat(error("METRICS tests max(avg(rate(network.bytes_in)))", tsdb), equalTo("""
            1:19: nested aggregations [avg(rate(network.bytes_in))] not allowed inside other aggregations\
             [max(avg(rate(network.bytes_in)))]
            line 1:23: the rate aggregate [rate(network.bytes_in)] can only be used within the metrics command\
             and inside another aggregate"""));
        assertThat(error("METRICS tests max(avg(rate(network.bytes_in)))", tsdb), equalTo("""
            1:19: nested aggregations [avg(rate(network.bytes_in))] not allowed inside other aggregations\
             [max(avg(rate(network.bytes_in)))]
            line 1:23: the rate aggregate [rate(network.bytes_in)] can only be used within the metrics command\
             and inside another aggregate"""));
    }

    public void testWeightedAvg() {
        assertEquals(
            "1:35: SECOND argument of [weighted_avg(v, null)] cannot be null or 0, received [null]",
            error("row v = [1, 2, 3] | stats w_avg = weighted_avg(v, null)")
        );
        assertEquals(
            "1:27: SECOND argument of [weighted_avg(salary, null)] cannot be null or 0, received [null]",
            error("from test | stats w_avg = weighted_avg(salary, null)")
        );
        assertEquals(
            "1:45: SECOND argument of [weighted_avg(v, w)] cannot be null or 0, received [null]",
            error("row v = [1, 2, 3], w = null | stats w_avg = weighted_avg(v, w)")
        );
        assertEquals(
            "1:44: SECOND argument of [weighted_avg(salary, w)] cannot be null or 0, received [null]",
            error("from test | eval w = null |  stats w_avg = weighted_avg(salary, w)")
        );
        assertEquals(
            "1:51: SECOND argument of [weighted_avg(salary, w)] cannot be null or 0, received [null]",
            error("from test | eval w = null + null |  stats w_avg = weighted_avg(salary, w)")
        );
        assertEquals(
            "1:35: SECOND argument of [weighted_avg(v, 0)] cannot be null or 0, received [0]",
            error("row v = [1, 2, 3] | stats w_avg = weighted_avg(v, 0)")
        );
        assertEquals(
            "1:27: SECOND argument of [weighted_avg(salary, 0.0)] cannot be null or 0, received [0.0]",
            error("from test | stats w_avg = weighted_avg(salary, 0.0)")
        );
    }

    public void testMatchInsideEval() throws Exception {
        assumeTrue("Match operator is available just for snapshots", Build.current().isSnapshot());

        assertEquals("1:36: EVAL does not support MATCH expressions", error("row title = \"brown fox\" | eval x = title match \"fox\" "));
    }

    public void testMatchFilter() throws Exception {
        assumeTrue("Match operator is available just for snapshots", Build.current().isSnapshot());

        assertEquals(
            "1:63: MATCH requires a mapped index field, found [name]",
            error("from test | eval name = concat(first_name, last_name) | where name match \"Anna\"")
        );

        assertEquals(
            "1:19: MATCH requires a text or keyword field, but [salary] has type [integer]",
            error("from test | where salary match \"100\"")
        );

        assertEquals(
            "1:19: Invalid condition using MATCH",
            error("from test | where first_name match \"Anna\" or starts_with(first_name, \"Anne\")")
        );

        assertEquals(
            "1:51: Invalid condition using MATCH",
            error("from test | eval new_salary = salary + 10 | where first_name match \"Anna\" OR new_salary > 100")
        );

        assertEquals(
            "1:45: MATCH requires a mapped index field, found [fn]",
            error("from test | rename first_name as fn | where fn match \"Anna\"")
        );
    }

    private String error(String query) {
        return error(query, defaultAnalyzer);
    }

    private String error(String query, Object... params) {
        return error(query, defaultAnalyzer, params);
    }

    private String error(String query, Analyzer analyzer, Object... params) {
        List<QueryParam> parameters = new ArrayList<>();
        for (Object param : params) {
            if (param == null) {
                parameters.add(new QueryParam(null, null, NULL));
            } else if (param instanceof String) {
                parameters.add(new QueryParam(null, param, KEYWORD));
            } else if (param instanceof Number) {
                parameters.add(new QueryParam(null, param, DataType.fromJava(param)));
            } else {
                throw new IllegalArgumentException("VerifierTests don't support params of type " + param.getClass());
            }
        }
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyzer.analyze(parser.createStatement(query, new QueryParams(parameters)))
        );
        String message = e.getMessage();
        assertTrue(message.startsWith("Found "));
        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
