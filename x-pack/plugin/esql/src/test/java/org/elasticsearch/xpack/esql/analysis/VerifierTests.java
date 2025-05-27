/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.Build;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MultiMatch;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.matchesRegex;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class VerifierTests extends ESTestCase {

    private static final EsqlParser parser = new EsqlParser();
    private final Analyzer defaultAnalyzer = AnalyzerTestUtils.expandedDefaultAnalyzer();
    private final Analyzer tsdb = AnalyzerTestUtils.analyzer(AnalyzerTestUtils.tsdbIndexResolution());

    private final List<String> TIME_DURATIONS = List.of("millisecond", "second", "minute", "hour");
    private final List<String> DATE_PERIODS = List.of("day", "week", "month", "year");

    public void testIncompatibleTypesInMathOperation() {
        assertEquals(
            "1:40: second argument of [a + c] must be [date_nanos, datetime or numeric], found value [c] type [keyword]",
            error("row a = 1, b = 2, c = \"xxx\" | eval y = a + c")
        );
        assertEquals(
            "1:40: second argument of [a - c] must be [date_nanos, datetime or numeric], found value [c] type [keyword]",
            error("row a = 1, b = 2, c = \"xxx\" | eval y = a - c")
        );
    }

    public void testUnsupportedAndMultiTypedFields() {
        final String unsupported = "unsupported";
        final String multiTyped = "multi_typed";

        EsField unsupportedField = new UnsupportedEsField(unsupported, List.of("flattened"));
        // Use linked maps/sets to fix the order in the error message.
        LinkedHashSet<String> ipIndices = new LinkedHashSet<>();
        ipIndices.add("test1");
        ipIndices.add("test2");
        ipIndices.add("test3");
        ipIndices.add("test4");
        ipIndices.add("test5");
        LinkedHashMap<String, Set<String>> typesToIndices = new LinkedHashMap<>();
        typesToIndices.put("ip", ipIndices);
        typesToIndices.put("keyword", Set.of("test6"));
        EsField multiTypedField = new InvalidMappedField(multiTyped, typesToIndices);

        // Also add an unsupported/multityped field under the names `int` and `double` so we can use `LOOKUP int_number_names ...` and
        // `LOOKUP double_number_names` without renaming the fields first.
        IndexResolution indexWithUnsupportedAndMultiTypedField = IndexResolution.valid(
            new EsIndex(
                "test*",
                Map.of(unsupported, unsupportedField, multiTyped, multiTypedField, "int", unsupportedField, "double", multiTypedField)
            )
        );
        Analyzer analyzer = AnalyzerTestUtils.analyzer(indexWithUnsupportedAndMultiTypedField);

        assertEquals(
            "1:22: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | dissect unsupported \"%{foo}\"", analyzer)
        );
        assertEquals(
            "1:22: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | dissect multi_typed \"%{foo}\"", analyzer)
        );

        assertEquals(
            "1:19: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | grok unsupported \"%{WORD:foo}\"", analyzer)
        );
        assertEquals(
            "1:19: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | grok multi_typed \"%{WORD:foo}\"", analyzer)
        );

        assertEquals(
            "1:36: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | enrich client_cidr on unsupported", analyzer)
        );
        assertEquals(
            "1:36: Unsupported type [unsupported] for enrich matching field [multi_typed];"
                + " only [keyword, text, ip, long, integer, float, double, datetime] allowed for type [range]",
            error("from test* | enrich client_cidr on multi_typed", analyzer)
        );

        assertEquals(
            "1:23: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | eval x = unsupported", analyzer)
        );
        assertEquals(
            "1:23: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | eval x = multi_typed", analyzer)
        );

        assertEquals(
            "1:32: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | eval x = to_lower(unsupported)", analyzer)
        );
        assertEquals(
            "1:32: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | eval x = to_lower(multi_typed)", analyzer)
        );

        assertEquals(
            "1:32: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | stats count(1) by unsupported", analyzer)
        );
        assertEquals(
            "1:32: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | stats count(1) by multi_typed", analyzer)
        );
        if (EsqlCapabilities.Cap.INLINESTATS.isEnabled()) {
            assertEquals(
                "1:38: Cannot use field [unsupported] with unsupported type [flattened]",
                error("from test* | inlinestats count(1) by unsupported", analyzer)
            );
            assertEquals(
                "1:38: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                    + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
                error("from test* | inlinestats count(1) by multi_typed", analyzer)
            );
        }

        assertEquals(
            "1:27: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | stats values(unsupported)", analyzer)
        );
        assertEquals(
            "1:27: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | stats values(multi_typed)", analyzer)
        );
        if (EsqlCapabilities.Cap.INLINESTATS.isEnabled()) {
            assertEquals(
                "1:33: Cannot use field [unsupported] with unsupported type [flattened]",
                error("from test* | inlinestats values(unsupported)", analyzer)
            );
            assertEquals(
                "1:33: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                    + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
                error("from test* | inlinestats values(multi_typed)", analyzer)
            );
        }

        assertEquals(
            "1:27: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | stats values(unsupported)", analyzer)
        );
        assertEquals(
            "1:27: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | stats values(multi_typed)", analyzer)
        );

        if (EsqlCapabilities.Cap.LOOKUP_V4.isEnabled()) {
            // LOOKUP with unsupported type
            assertEquals(
                "1:43: column type mismatch, table column was [integer] and original column was [unsupported]",
                error("from test* | lookup_🐔 int_number_names on int", analyzer)
            );
            // LOOKUP with multi-typed field
            assertEquals(
                "1:46: column type mismatch, table column was [double] and original column was [unsupported]",
                error("from test* | lookup_🐔 double_number_names on double", analyzer)
            );
        }

        assertEquals(
            "1:24: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | mv_expand unsupported", analyzer)
        );
        assertEquals(
            "1:24: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | mv_expand multi_typed", analyzer)
        );

        assertEquals(
            "1:21: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | rename unsupported as x", analyzer)
        );
        assertEquals(
            "1:21: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | rename multi_typed as x", analyzer)
        );

        assertEquals(
            "1:19: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | sort unsupported asc", analyzer)
        );
        assertEquals(
            "1:19: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | sort multi_typed desc", analyzer)
        );

        assertEquals(
            "1:20: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | where unsupported is null", analyzer)
        );
        assertEquals(
            "1:20: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | where multi_typed is not null", analyzer)
        );

        for (String functionName : List.of("to_timeduration", "to_dateperiod")) {
            String lineNumber = functionName.equalsIgnoreCase("to_timeduration") ? "47" : "45";
            String errorType = functionName.equalsIgnoreCase("to_timeduration") ? "time_duration" : "date_period";
            assertEquals(
                "1:" + lineNumber + ": Cannot use field [unsupported] with unsupported type [flattened]",
                error("from test* | eval x = now() + " + functionName + "(unsupported)", analyzer)
            );
            assertEquals(
                "1:" + lineNumber + ": argument of [" + functionName + "(multi_typed)] must be a constant, received [multi_typed]",
                error("from test* | eval x = now() + " + functionName + "(multi_typed)", analyzer)
            );
            assertThat(
                error("from test* | eval x = unsupported, y = now() + " + functionName + "(x)", analyzer),
                containsString("1:23: Cannot use field [unsupported] with unsupported type [flattened]")
            );
            assertThat(
                error("from test* | eval x = multi_typed, y = now() + " + functionName + "(x)", analyzer),
                containsString(
                    "1:48: argument of ["
                        + functionName
                        + "(x)] must be ["
                        + errorType
                        + " or string], "
                        + "found value [x] type [unsupported]"
                )
            );
        }
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
            "1:31: second argument of [round(a, b)] must be [whole number except unsigned_long or counter types], "
                + "found value [b] type [keyword]",
            error("row a = 1, b = \"c\" | eval x = round(a, b)")
        );
        assertEquals(
            "1:31: second argument of [round(a, 3.5)] must be [whole number except unsigned_long or counter types], "
                + "found value [3.5] type [double]",
            error("row a = 1, b = \"c\" | eval x = round(a, 3.5)")
        );
    }

    public void testImplicitCastingErrorMessages() {
        assertEquals("1:23: Cannot convert string [c] to [LONG], error [Cannot parse number [c]]", error("row a = round(123.45, \"c\")"));
        assertEquals(
            "1:27: Cannot convert string [c] to [DOUBLE], error [Cannot parse number [c]]",
            error("row a = 1 | eval x = acos(\"c\")")
        );
        assertEquals(
            "1:33: Cannot convert string [c] to [DOUBLE], error [Cannot parse number [c]]\n"
                + "line 1:38: Cannot convert string [a] to [LONG], error [Cannot parse number [a]]",
            error("row a = 1 | eval x = round(acos(\"c\"),\"a\")")
        );
        assertEquals(
            "1:63: Cannot convert string [x] to [INTEGER], error [Cannot parse number [x]]",
            error("row ip4 = to_ip(\"1.2.3.4\") | eval ip4_prefix = ip_prefix(ip4, \"x\", 0)")
        );
        assertEquals(
            "1:42: Cannot convert string [a] to [DOUBLE], error [Cannot parse number [a]]",
            error("ROW a=[3, 5, 1, 6] | EVAL avg_a = MV_AVG(\"a\")")
        );
        assertEquals(
            "1:19: Unknown column [languages.*], did you mean any of [languages, languages.byte, languages.long, languages.short]?",
            error("from test | where `languages.*` in (1, 2)")
        );
        assertEquals("1:22: Unknown function [func]", error("from test | eval x = func(languages) | where x in (1, 2)"));
        assertEquals(
            "1:32: Unknown column [languages.*], did you mean any of [languages, languages.byte, languages.long, languages.short]?",
            error("from test | eval x = coalesce( `languages.*`, languages, 0 )")
        );
        String error = error("from test | eval x = func(languages) | eval y = coalesce(x, languages, 0 )");
        assertThat(error, containsString("function [func]"));
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

    public void testAggFilterOnNonAggregates() {
        assertEquals(
            "1:36: WHERE clause allowed only for aggregate functions, none found in [emp_no + 1 where languages > 1]",
            error("from test | stats emp_no + 1 where languages > 1 by emp_no")
        );
        assertEquals(
            "1:53: WHERE clause allowed only for aggregate functions, none found in [abs(emp_no + languages) % 2 WHERE languages > 1]",
            error("from test | stats abs(emp_no + languages) % 2 WHERE languages > 1 by emp_no, languages")
        );
    }

    public void testAggFilterOnBucketingOrAggFunctions() {
        // query passes when the bucket function is part of the BY clause
        query("from test | stats max(languages) WHERE bucket(salary, 10) > 1 by bucket(salary, 10)");

        // but fails if it's different
        assertEquals(
            "1:32: can only use grouping function [bucket(a, 3)] as part of the BY clause",
            error("row a = 1 | stats sum(a) where bucket(a, 3) > -1 by bucket(a,2)")
        );

        assertEquals(
            "1:40: can only use grouping function [bucket(salary, 10)] as part of the BY clause",
            error("from test | stats max(languages) WHERE bucket(salary, 10) > 1 by emp_no")
        );

        assertEquals(
            "1:40: cannot use aggregate function [max(salary)] in aggregate WHERE clause [max(languages) WHERE max(salary) > 1]",
            error("from test | stats max(languages) WHERE max(salary) > 1 by emp_no")
        );

        assertEquals(
            "1:40: cannot use aggregate function [max(salary)] in aggregate WHERE clause [max(languages) WHERE max(salary) + 2 > 1]",
            error("from test | stats max(languages) WHERE max(salary) + 2 > 1 by emp_no")
        );

        assertEquals("1:60: Unknown column [m]", error("from test | stats m = max(languages), min(languages) WHERE m + 2 > 1 by emp_no"));
    }

    public void testAggWithNonBooleanFilter() {
        for (String filter : List.of("\"true\"", "1", "1 + 0", "concat(\"a\", \"b\")")) {
            String type = (filter.equals("1") || filter.equals("1 + 0")) ? "INTEGER" : "KEYWORD";
            assertEquals("1:19: Condition expression needs to be boolean, found [" + type + "]", error("from test | where " + filter));
            for (String by : List.of("", " by languages", " by bucket(salary, 10)")) {
                assertEquals(
                    "1:34: Condition expression needs to be boolean, found [" + type + "]",
                    error("from test | stats count(*) where " + filter + by)
                );
            }
        }
    }

    public void testGroupingInsideAggsAsAgg() {
        assertEquals(
            "1:18: can only use grouping function [bucket(emp_no, 5.)] as part of the BY clause",
            error("from test| stats bucket(emp_no, 5.) by emp_no")
        );
        assertEquals(
            "1:18: can only use grouping function [bucket(emp_no, 5.)] as part of the BY clause",
            error("from test| stats bucket(emp_no, 5.)")
        );
        assertEquals(
            "1:18: can only use grouping function [bucket(emp_no, 5.)] as part of the BY clause",
            error("from test| stats bucket(emp_no, 5.) by bucket(emp_no, 6.)")
        );
        assertEquals(
            "1:22: can only use grouping function [bucket(emp_no, 5.)] as part of the BY clause",
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

    public void testInvalidBucketCalls() {
        assertThat(
            error("from test | stats max(emp_no) by bucket(emp_no, 5, \"2000-01-01\")"),
            containsString(
                "function expects exactly four arguments when the first one is of type [INTEGER] and the second of type [INTEGER]"
            )
        );

        assertThat(
            error("from test | stats max(emp_no) by bucket(emp_no, 1 week, \"2000-01-01\")"),
            containsString(
                "second argument of [bucket(emp_no, 1 week, \"2000-01-01\")] must be [numeric], found value [1 week] type [date_period]"
            )
        );

        assertThat(
            error("from test | stats max(emp_no) by bucket(hire_date, 5.5, \"2000-01-01\")"),
            containsString(
                "second argument of [bucket(hire_date, 5.5, \"2000-01-01\")] must be [integral, date_period or time_duration], "
                    + "found value [5.5] type [double]"
            )
        );

        assertThat(
            error("from test | stats max(emp_no) by bucket(hire_date, 5, 1 day, 1 month)"),
            containsString(
                "third argument of [bucket(hire_date, 5, 1 day, 1 month)] must be [datetime or string], "
                    + "found value [1 day] type [date_period]"
            )
        );

        assertThat(
            error("from test | stats max(emp_no) by bucket(hire_date, 5, \"2000-01-01\", 1 month)"),
            containsString(
                "fourth argument of [bucket(hire_date, 5, \"2000-01-01\", 1 month)] must be [datetime or string], "
                    + "found value [1 month] type [date_period]"
            )
        );

        assertThat(
            error("from test | stats max(emp_no) by bucket(hire_date, 5, \"2000-01-01\")"),
            containsString(
                "function expects exactly four arguments when the first one is of type [DATETIME] and the second of type [INTEGER]"
            )
        );

        assertThat(
            error("from test | stats max(emp_no) by bucket(emp_no, \"5\")"),
            containsString("second argument of [bucket(emp_no, \"5\")] must be [numeric], found value [\"5\"] type [keyword]")
        );

        assertThat(
            error("from test | stats max(emp_no) by bucket(hire_date, \"5\")"),
            containsString(
                "second argument of [bucket(hire_date, \"5\")] must be [integral, date_period or time_duration], "
                    + "found value [\"5\"] type [keyword]"
            )
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
            "1:22: column [languages] cannot be used as an aggregate once declared in the STATS BY grouping key [l = languages % 3]",
            error("from test| stats l = languages + 3 by l = languages % 3, l = languages, l = languages % 2 | keep l")
        );

        assertEquals(
            "1:22: column [languages] cannot be used as an aggregate once declared in the STATS BY grouping key [l = languages % 3]",
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
            "1:19: argument of [sum(hire_date)] must be [aggregate_metric_double or numeric except unsigned_long or counter types],"
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
        for (var unit : TIME_DURATIONS) {
            assertEquals("1:9: cannot use [1 " + unit + "] directly in a row assignment", error("row a = 1 " + unit));
            assertEquals(
                "1:9: cannot use [1 " + unit + "::time_duration] directly in a row assignment",
                error("row a = 1 " + unit + "::time_duration")
            );
            assertEquals(
                "1:9: cannot use [\"1 " + unit + "\"::time_duration] directly in a row assignment",
                error("row a = \"1 " + unit + "\"::time_duration")
            );
            assertEquals(
                "1:9: cannot use [to_timeduration(1 " + unit + ")] directly in a row assignment",
                error("row a = to_timeduration(1 " + unit + ")")
            );
            assertEquals(
                "1:9: cannot use [to_timeduration(\"1 " + unit + "\")] directly in a row assignment",
                error("row a = to_timeduration(\"1 " + unit + "\")")
            );
        }
        for (var unit : DATE_PERIODS) {
            assertEquals("1:9: cannot use [1 " + unit + "] directly in a row assignment", error("row a = 1 " + unit));
            assertEquals(
                "1:9: cannot use [1 " + unit + "::date_period] directly in a row assignment",
                error("row a = 1 " + unit + "::date_period")
            );
            assertEquals(
                "1:9: cannot use [\"1 " + unit + "\"::date_period] directly in a row assignment",
                error("row a = \"1 " + unit + "\"::date_period")
            );
            assertEquals(
                "1:9: cannot use [to_dateperiod(1 " + unit + ")] directly in a row assignment",
                error("row a = to_dateperiod(1 " + unit + ")")
            );
            assertEquals(
                "1:9: cannot use [to_dateperiod(\"1 " + unit + "\")] directly in a row assignment",
                error("row a = to_dateperiod(\"1 " + unit + "\")")
            );
        }
    }

    public void testSubtractDateTimeFromTemporal() {
        for (var unit : TIME_DURATIONS) {
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] "
                    + "from a [TIME_DURATION] amount [1 "
                    + unit
                    + "]",
                error("row 1 " + unit + " - now() ")
            );
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] "
                    + "from a [TIME_DURATION] amount [1 "
                    + unit
                    + "::time_duration]",
                error("row 1 " + unit + "::time_duration" + " - now() ")
            );
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] "
                    + "from a [TIME_DURATION] amount [\"1 "
                    + unit
                    + "\"::time_duration]",
                error("row \"1 " + unit + "\"::time_duration" + " - now() ")
            );
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] "
                    + "from a [TIME_DURATION] amount [to_timeduration(1 "
                    + unit
                    + ")]",
                error("row to_timeduration(1 " + unit + ") - now() ")
            );
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] "
                    + "from a [TIME_DURATION] amount [to_timeduration(\"1 "
                    + unit
                    + "\")]",
                error("row to_timeduration(\"1 " + unit + "\") - now() ")
            );
        }
        for (var unit : DATE_PERIODS) {
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] "
                    + "from a [DATE_PERIOD] amount [1 "
                    + unit
                    + "]",
                error("row 1 " + unit + " - now() ")
            );
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] "
                    + "from a [DATE_PERIOD] amount [1 "
                    + unit
                    + "::date_period]",
                error("row 1 " + unit + "::date_period" + " - now() ")
            );
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] "
                    + "from a [DATE_PERIOD] amount [\"1 "
                    + unit
                    + "\"::date_period]",
                error("row \"1 " + unit + "\"::date_period" + " - now() ")
            );
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] "
                    + "from a [DATE_PERIOD] amount [to_dateperiod(1 "
                    + unit
                    + ")]",
                error("row to_dateperiod(1 " + unit + ") - now() ")
            );
            assertEquals(
                "1:5: [-] arguments are in unsupported order: cannot subtract a [DATETIME] value [now()] "
                    + "from a [DATE_PERIOD] amount [to_dateperiod(\"1 "
                    + unit
                    + "\")]",
                error("row to_dateperiod(\"1 " + unit + "\") - now() ")
            );
        }
    }

    public void testPeriodAndDurationInEval() {
        for (var unit : TIME_DURATIONS) {
            assertEquals(
                "1:18: EVAL does not support type [time_duration] as the return data type of expression [1 " + unit + "]",
                error("row x = 1 | eval y = 1 " + unit)
            );
            assertEquals(
                "1:18: EVAL does not support type [time_duration] as the return data type of expression [1 " + unit + "::time_duration]",
                error("row x = 1 | eval y = 1 " + unit + "::time_duration")
            );
            assertEquals(
                "1:18: EVAL does not support type [time_duration] as the return data type of expression [\"1 "
                    + unit
                    + "\"::time_duration]",
                error("row x = 1 | eval y = \"1 " + unit + "\"::time_duration")
            );
            assertEquals(
                "1:18: EVAL does not support type [time_duration] as the return data type of expression [to_timeduration(1 " + unit + ")]",
                error("row x = 1 | eval y = to_timeduration(1 " + unit + ")")
            );
            assertEquals(
                "1:18: EVAL does not support type [time_duration] as the return data type of expression [to_timeduration(\"1 "
                    + unit
                    + "\")]",
                error("row x = 1 | eval y = to_timeduration(\"1 " + unit + "\")")
            );
        }
        for (var unit : DATE_PERIODS) {
            assertEquals(
                "1:18: EVAL does not support type [date_period] as the return data type of expression [1 " + unit + "]",
                error("row x = 1 | eval y = 1 " + unit)
            );
            assertEquals(
                "1:18: EVAL does not support type [date_period] as the return data type of expression [1 " + unit + "::date_period]",
                error("row x = 1 | eval y = 1 " + unit + "::date_period")
            );
            assertEquals(
                "1:18: EVAL does not support type [date_period] as the return data type of expression [\"1 " + unit + "\"::date_period]",
                error("row x = 1 | eval y = \"1 " + unit + "\"::date_period")
            );
            assertEquals(
                "1:18: EVAL does not support type [date_period] as the return data type of expression [to_dateperiod(1 " + unit + ")]",
                error("row x = 1 | eval y = to_dateperiod(1 " + unit + ")")
            );
            assertEquals(
                "1:18: EVAL does not support type [date_period] as the return data type of expression [to_dateperiod(\"1 " + unit + "\")]",
                error("row x = 1 | eval y = to_dateperiod(\"1 " + unit + "\")")
            );
        }
    }

    public void testFilterNonBoolField() {
        assertEquals("1:19: Condition expression needs to be boolean, found [INTEGER]", error("from test | where emp_no"));

        assertEquals(
            "1:19: Condition expression needs to be boolean, found [KEYWORD]",
            error("from test | where concat(first_name, \"foobar\")")
        );
    }

    public void testFilterNullField() {
        // `where null` should return empty result set
        query("from test | where null");

        // Value null of type `BOOLEAN`
        query("from test | where null::boolean");

        // Provide `NULL` type in `EVAL`
        query("from t | EVAL x = null | where x");

        // `to_string(null)` is of `KEYWORD` type null, resulting in `to_string(null) == "abc"` being of `BOOLEAN`
        query("from t | where to_string(null) == \"abc\"");

        // Other DataTypes can contain null values
        assertEquals("1:19: Condition expression needs to be boolean, found [KEYWORD]", error("from test | where null::string"));
        assertEquals("1:19: Condition expression needs to be boolean, found [INTEGER]", error("from test | where null::integer"));
        assertEquals(
            "1:45: Condition expression needs to be boolean, found [DATETIME]",
            error("from test | EVAL x = null::datetime | where x")
        );
    }

    public void testFilterDateConstant() {
        assertEquals("1:19: Condition expression needs to be boolean, found [DATE_PERIOD]", error("from test | where 1 year"));
        assertEquals(
            "1:19: Condition expression needs to be boolean, found [DATE_PERIOD]",
            error("from test | where \"1 year\"::date_period")
        );
        assertEquals(
            "1:19: Condition expression needs to be boolean, found [DATE_PERIOD]",
            error("from test | where to_dateperiod(\"1 year\")")
        );
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

    public void testMultipleAggsOutsideStats() {
        assertEquals(
            """
                1:71: aggregate function [avg(salary)] not allowed outside STATS command
                line 1:96: aggregate function [median(emp_no)] not allowed outside STATS command
                line 1:22: aggregate function [sum(salary)] not allowed outside STATS command
                line 1:39: aggregate function [avg(languages)] not allowed outside STATS command""",
            error("from test | eval s = sum(salary), l = avg(languages) | where salary > avg(salary) and emp_no > median(emp_no)")
        );
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

    public void testSourceSorting() {
        assertEquals("1:35: cannot sort on _source", error("from test metadata _source | sort _source"));
    }

    public void testCountersSorting() {
        Map<DataType, String> counterDataTypes = Map.of(
            COUNTER_DOUBLE,
            "network.message_in",
            COUNTER_INTEGER,
            "network.message_out",
            COUNTER_LONG,
            "network.bytes_out"
        );
        for (DataType counterDT : counterDataTypes.keySet()) {
            var fieldName = counterDataTypes.get(counterDT);
            assertEquals("1:18: cannot sort on " + counterDT.name().toLowerCase(Locale.ROOT), error("from test | sort " + fieldName, tsdb));
        }
    }

    public void testInlineImpossibleConvert() {
        assertEquals("1:5: argument of [false::ip] must be [ip or string], found value [false] type [boolean]", error("ROW false::ip"));
    }

    public void testAggregateOnCounter() {
        assertThat(
            error("FROM tests | STATS min(network.bytes_in)", tsdb),
            equalTo(
                "1:20: argument of [min(network.bytes_in)] must be"
                    + " [representable except unsigned_long and spatial types],"
                    + " found value [network.bytes_in] type [counter_long]"
            )
        );

        assertThat(
            error("FROM tests | STATS max(network.bytes_in)", tsdb),
            equalTo(
                "1:20: argument of [max(network.bytes_in)] must be"
                    + " [representable except unsigned_long and spatial types],"
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
            error("FROM tests | STATS " + agg_func + "(foobar) by BUCKET(hire_date, 10)"),
            matchesRegex(
                "1:\\d+: function expects exactly four arguments when the first one is of type \\[DATETIME]"
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
            equalTo("1:24: time_series aggregate[rate(network.bytes_in)] can only be used with the TS command")
        );
        assertThat(
            error("FROM tests | STATS rate(network.bytes_in)", tsdb),
            equalTo("1:20: time_series aggregate[rate(network.bytes_in)] can only be used with the TS command")
        );
        assertThat(
            error("FROM tests | STATS max_over_time(network.connections)", tsdb),
            equalTo("1:20: time_series aggregate[max_over_time(network.connections)] can only be used with the TS command")
        );
        assertThat(
            error("FROM tests | EVAL r = rate(network.bytes_in)", tsdb),
            equalTo("1:23: aggregate function [rate(network.bytes_in)] not allowed outside STATS command")
        );
    }

    public void testRateNotEnclosedInAggregate() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        assertThat(
            error("TS tests | STATS rate(network.bytes_in)", tsdb),
            equalTo("1:18: the rate aggregate [rate(network.bytes_in)] can only be used with the TS command and inside another aggregate")
        );
        assertThat(
            error("TS tests | STATS avg(rate(network.bytes_in)), rate(network.bytes_in)", tsdb),
            equalTo("1:47: the rate aggregate [rate(network.bytes_in)] can only be used with the TS command and inside another aggregate")
        );
        assertThat(error("TS tests | STATS max(avg(rate(network.bytes_in)))", tsdb), equalTo("""
            1:22: nested aggregations [avg(rate(network.bytes_in))] not allowed inside other aggregations\
             [max(avg(rate(network.bytes_in)))]
            line 1:26: the rate aggregate [rate(network.bytes_in)] can only be used with the TS command\
             and inside another aggregate"""));
        assertThat(error("TS tests | STATS max(avg(rate(network.bytes_in)))", tsdb), equalTo("""
            1:22: nested aggregations [avg(rate(network.bytes_in))] not allowed inside other aggregations\
             [max(avg(rate(network.bytes_in)))]
            line 1:26: the rate aggregate [rate(network.bytes_in)] can only be used with the TS command\
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
        assertEquals(
            "1:36: [:] operator is only supported in WHERE and STATS commands\n"
                + "line 1:36: [:] operator cannot operate on [title], which is not a field from an index mapping",
            error("row title = \"brown fox\" | eval x = title:\"fox\" ")
        );
    }

    public void testMatchFunctionNotAllowedAfterCommands() throws Exception {
        assertEquals(
            "1:24: [MATCH] function cannot be used after LIMIT",
            error("from test | limit 10 | where match(first_name, \"Anna\")")
        );
        assertEquals(
            "1:47: [MATCH] function cannot be used after STATS",
            error("from test | STATS c = AVG(salary) BY gender | where match(gender, \"F\")")
        );
    }

    public void testMatchFunctionAndOperatorHaveCorrectErrorMessages() throws Exception {
        assertEquals(
            "1:24: [MATCH] function cannot be used after LIMIT",
            error("from test | limit 10 | where match(first_name, \"Anna\")")
        );
        assertEquals(
            "1:24: [MATCH] function cannot be used after LIMIT",
            error("from test | limit 10 | where match ( first_name, \"Anna\" ) ")
        );
        assertEquals("1:24: [:] operator cannot be used after LIMIT", error("from test | limit 10 | where first_name:\"Anna\""));
        assertEquals("1:24: [:] operator cannot be used after LIMIT", error("from test | limit 10 | where first_name : \"Anna\""));
    }

    // These should pass eventually once we lift some restrictions on match function
    public void testMatchWithNonIndexedColumnCurrentlyUnsupported() {
        assertEquals(
            "1:67: [MATCH] function cannot operate on [initial], which is not a field from an index mapping",
            error("from test | eval initial = substring(first_name, 1) | where match(initial, \"A\")")
        );
        assertEquals(
            "1:67: [MATCH] function cannot operate on [text], which is not a field from an index mapping",
            error("from test | eval text=concat(first_name, last_name) | where match(text, \"cat\")")
        );
    }

    public void testMatchFunctionIsNotNullable() {
        assertEquals(
            "1:48: [MATCH] function cannot operate on [text::keyword], which is not a field from an index mapping",
            error("row n = null | eval text = n + 5 | where match(text::keyword, \"Anna\")")
        );
    }

    public void testQueryStringFunctionsNotAllowedAfterCommands() throws Exception {
        // Source commands
        assertEquals("1:13: [QSTR] function cannot be used after SHOW", error("show info | where qstr(\"8.16.0\")"));
        assertEquals("1:17: [QSTR] function cannot be used after ROW", error("row a= \"Anna\" | where qstr(\"Anna\")"));

        // Processing commands
        assertEquals(
            "1:43: [QSTR] function cannot be used after DISSECT",
            error("from test | dissect first_name \"%{foo}\" | where qstr(\"Connection\")")
        );
        assertEquals("1:27: [QSTR] function cannot be used after DROP", error("from test | drop emp_no | where qstr(\"Anna\")"));
        assertEquals(
            "1:71: [QSTR] function cannot be used after ENRICH",
            error("from test | enrich languages on languages with lang = language_name | where qstr(\"Anna\")")
        );
        assertEquals("1:26: [QSTR] function cannot be used after EVAL", error("from test | eval z = 2 | where qstr(\"Anna\")"));
        assertEquals(
            "1:44: [QSTR] function cannot be used after GROK",
            error("from test | grok last_name \"%{WORD:foo}\" | where qstr(\"Anna\")")
        );
        assertEquals("1:27: [QSTR] function cannot be used after KEEP", error("from test | keep emp_no | where qstr(\"Anna\")"));
        assertEquals("1:24: [QSTR] function cannot be used after LIMIT", error("from test | limit 10 | where qstr(\"Anna\")"));
        assertEquals(
            "1:35: [QSTR] function cannot be used after MV_EXPAND",
            error("from test | mv_expand last_name | where qstr(\"Anna\")")
        );
        assertEquals(
            "1:45: [QSTR] function cannot be used after RENAME",
            error("from test | rename last_name as full_name | where qstr(\"Anna\")")
        );
        assertEquals(
            "1:52: [QSTR] function cannot be used after STATS",
            error("from test | STATS c = COUNT(emp_no) BY languages | where qstr(\"Anna\")")
        );

        // Some combination of processing commands
        assertEquals(
            "1:38: [QSTR] function cannot be used after LIMIT",
            error("from test | keep emp_no | limit 10 | where qstr(\"Anna\")")
        );
        assertEquals(
            "1:46: [QSTR] function cannot be used after MV_EXPAND",
            error("from test | limit 10 | mv_expand last_name | where qstr(\"Anna\")")
        );
        assertEquals(
            "1:52: [QSTR] function cannot be used after KEEP",
            error("from test | mv_expand last_name | keep last_name | where qstr(\"Anna\")")
        );
        assertEquals(
            "1:77: [QSTR] function cannot be used after RENAME",
            error("from test | STATS c = COUNT(emp_no) BY languages | rename c as total_emps | where qstr(\"Anna\")")
        );
        assertEquals(
            "1:54: [QSTR] function cannot be used after KEEP",
            error("from test | rename last_name as name | keep emp_no | where qstr(\"Anna\")")
        );
    }

    public void testKqlFunctionsNotAllowedAfterCommands() throws Exception {
        // Source commands
        assertEquals("1:13: [KQL] function cannot be used after SHOW", error("show info | where kql(\"8.16.0\")"));
        assertEquals("1:17: [KQL] function cannot be used after ROW", error("row a= \"Anna\" | where kql(\"Anna\")"));

        // Processing commands
        assertEquals(
            "1:43: [KQL] function cannot be used after DISSECT",
            error("from test | dissect first_name \"%{foo}\" | where kql(\"Connection\")")
        );
        assertEquals("1:27: [KQL] function cannot be used after DROP", error("from test | drop emp_no | where kql(\"Anna\")"));
        assertEquals(
            "1:71: [KQL] function cannot be used after ENRICH",
            error("from test | enrich languages on languages with lang = language_name | where kql(\"Anna\")")
        );
        assertEquals("1:26: [KQL] function cannot be used after EVAL", error("from test | eval z = 2 | where kql(\"Anna\")"));
        assertEquals(
            "1:44: [KQL] function cannot be used after GROK",
            error("from test | grok last_name \"%{WORD:foo}\" | where kql(\"Anna\")")
        );
        assertEquals("1:27: [KQL] function cannot be used after KEEP", error("from test | keep emp_no | where kql(\"Anna\")"));
        assertEquals("1:24: [KQL] function cannot be used after LIMIT", error("from test | limit 10 | where kql(\"Anna\")"));
        assertEquals("1:35: [KQL] function cannot be used after MV_EXPAND", error("from test | mv_expand last_name | where kql(\"Anna\")"));
        assertEquals(
            "1:45: [KQL] function cannot be used after RENAME",
            error("from test | rename last_name as full_name | where kql(\"Anna\")")
        );
        assertEquals(
            "1:52: [KQL] function cannot be used after STATS",
            error("from test | STATS c = COUNT(emp_no) BY languages | where kql(\"Anna\")")
        );

        // Some combination of processing commands
        assertEquals("1:38: [KQL] function cannot be used after LIMIT", error("from test | keep emp_no | limit 10 | where kql(\"Anna\")"));
        assertEquals(
            "1:46: [KQL] function cannot be used after MV_EXPAND",
            error("from test | limit 10 | mv_expand last_name | where kql(\"Anna\")")
        );
        assertEquals(
            "1:52: [KQL] function cannot be used after KEEP",
            error("from test | mv_expand last_name | keep last_name | where kql(\"Anna\")")
        );
        assertEquals(
            "1:77: [KQL] function cannot be used after RENAME",
            error("from test | STATS c = COUNT(emp_no) BY languages | rename c as total_emps | where kql(\"Anna\")")
        );
        assertEquals(
            "1:54: [KQL] function cannot be used after DROP",
            error("from test | rename last_name as name | drop emp_no | where kql(\"Anna\")")
        );
    }

    public void testQueryStringFunctionOnlyAllowedInWhere() throws Exception {
        assertEquals("1:9: [QSTR] function is only supported in WHERE and STATS commands", error("row a = qstr(\"Anna\")"));
        checkFullTextFunctionsOnlyAllowedInWhere("QSTR", "qstr(\"Anna\")", "function");
    }

    public void testKqlFunctionOnlyAllowedInWhere() throws Exception {
        assertEquals("1:9: [KQL] function is only supported in WHERE and STATS commands", error("row a = kql(\"Anna\")"));
        checkFullTextFunctionsOnlyAllowedInWhere("KQL", "kql(\"Anna\")", "function");
    }

    public void testMatchFunctionOnlyAllowedInWhere() throws Exception {
        checkFullTextFunctionsOnlyAllowedInWhere("MATCH", "match(first_name, \"Anna\")", "function");
    }

    public void testTermFunctionOnlyAllowedInWhere() throws Exception {
        assumeTrue("term function capability not available", EsqlCapabilities.Cap.TERM_FUNCTION.isEnabled());
        checkFullTextFunctionsOnlyAllowedInWhere("Term", "term(first_name, \"Anna\")", "function");
    }

    public void testMatchOperatornOnlyAllowedInWhere() throws Exception {
        checkFullTextFunctionsOnlyAllowedInWhere(":", "first_name:\"Anna\"", "operator");
    }

    private void checkFullTextFunctionsOnlyAllowedInWhere(String functionName, String functionInvocation, String functionType)
        throws Exception {
        assertEquals(
            "1:22: [" + functionName + "] " + functionType + " is only supported in WHERE and STATS commands",
            error("from test | eval y = " + functionInvocation)
        );
        assertEquals(
            "1:18: [" + functionName + "] " + functionType + " is only supported in WHERE and STATS commands",
            error("from test | sort " + functionInvocation + " asc")
        );
        assertEquals(
            "1:47: [" + functionName + "] " + functionType + " is only supported in WHERE and STATS commands",
            error("from test | stats max_salary = max(salary) by " + functionInvocation)
        );
    }

    public void testQueryStringFunctionArgNotNullOrConstant() throws Exception {
        assertEquals(
            "1:19: first argument of [qstr(first_name)] must be a constant, received [first_name]",
            error("from test | where qstr(first_name)")
        );
        assertEquals("1:19: first argument of [qstr(null)] cannot be null, received [null]", error("from test | where qstr(null)"));
        // Other value types are tested in QueryStringFunctionTests
    }

    public void testKqlFunctionArgNotNullOrConstant() throws Exception {
        assertEquals(
            "1:19: argument of [kql(first_name)] must be a constant, received [first_name]",
            error("from test | where kql(first_name)")
        );
        assertEquals("1:19: argument of [kql(null)] cannot be null, received [null]", error("from test | where kql(null)"));
        // Other value types are tested in KqlFunctionTests
    }

    public void testQueryStringWithDisjunctions() {
        checkWithDisjunctions("QSTR", "qstr(\"first_name: Anna\")", "function");
    }

    public void testKqlFunctionWithDisjunctions() {
        checkWithDisjunctions("KQL", "kql(\"first_name: Anna\")", "function");
    }

    public void testMatchFunctionWithDisjunctions() {
        checkWithDisjunctions("MATCH", "match(first_name, \"Anna\")", "function");
    }

    public void testTermFunctionWithDisjunctions() {
        assumeTrue("term function capability not available", EsqlCapabilities.Cap.TERM_FUNCTION.isEnabled());
        checkWithDisjunctions("Term", "term(first_name, \"Anna\")", "function");
    }

    public void testMatchOperatorWithDisjunctions() {
        checkWithDisjunctions(":", "first_name : \"Anna\"", "operator");
    }

    private void checkWithDisjunctions(String functionName, String functionInvocation, String functionType) {
        query("from test | where " + functionInvocation + " or length(first_name) > 12");
        query(
            "from test | where ("
                + functionInvocation
                + " or first_name is not null) or (length(first_name) > 12 and match(last_name, \"Smith\"))"
        );
        query("from test | where " + functionInvocation + " or (last_name is not null and first_name is null)");
    }

    public void testFullTextFunctionsDisjunctions() {
        checkWithFullTextFunctionsDisjunctions("match(last_name, \"Smith\")");
        checkWithFullTextFunctionsDisjunctions("multi_match(first_name, last_name, \"Smith\")");
        checkWithFullTextFunctionsDisjunctions("last_name : \"Smith\"");
        checkWithFullTextFunctionsDisjunctions("qstr(\"last_name: Smith\")");
        checkWithFullTextFunctionsDisjunctions("kql(\"last_name: Smith\")");
    }

    private void checkWithFullTextFunctionsDisjunctions(String functionInvocation) {

        // Disjunctions with non-pushable functions - scoring
        query("from test | where " + functionInvocation + " or length(first_name) > 10");
        query("from test | where match(last_name, \"Anneke\") or (" + functionInvocation + " and length(first_name) > 10)");
        query(
            "from test | where ("
                + functionInvocation
                + " and length(first_name) > 0) or (match(last_name, \"Anneke\") and length(first_name) > 10)"
        );

        // Disjunctions with non-pushable functions - no scoring
        query("from test | where " + functionInvocation + " or length(first_name) > 10");
        query("from test | where match(last_name, \"Anneke\") or (" + functionInvocation + " and length(first_name) > 10)");
        query(
            "from test | where ("
                + functionInvocation
                + " and length(first_name) > 0) or (match(last_name, \"Anneke\") and length(first_name) > 10)"
        );

        // Disjunctions with full text functions - no scoring
        query("from test | where " + functionInvocation + " or match(first_name, \"Anna\")");
        query("from test | where " + functionInvocation + " or not match(first_name, \"Anna\")");
        query("from test | where (" + functionInvocation + " or match(first_name, \"Anna\")) and length(first_name) > 10");
        query("from test | where (" + functionInvocation + " or match(first_name, \"Anna\")) and match(last_name, \"Smith\")");
        query("from test | where " + functionInvocation + " or (match(first_name, \"Anna\") and match(last_name, \"Smith\"))");

        // Disjunctions with full text functions - scoring
        query("from test metadata _score | where " + functionInvocation + " or match(first_name, \"Anna\")");
        query("from test metadata _score | where " + functionInvocation + " or not match(first_name, \"Anna\")");
        query("from test metadata _score | where (" + functionInvocation + " or match(first_name, \"Anna\")) and length(first_name) > 10");
        query(
            "from test metadata _score | where (" + functionInvocation + " or match(first_name, \"Anna\")) and match(last_name, \"Smith\")"
        );
        query(
            "from test metadata _score | where " + functionInvocation + " or (match(first_name, \"Anna\") and match(last_name, \"Smith\"))"
        );

    }

    public void testQueryStringFunctionWithNonBooleanFunctions() {
        checkFullTextFunctionsWithNonBooleanFunctions("QSTR", "qstr(\"first_name: Anna\")", "function");
    }

    public void testKqlFunctionWithNonBooleanFunctions() {
        checkFullTextFunctionsWithNonBooleanFunctions("KQL", "kql(\"first_name: Anna\")", "function");
    }

    public void testMatchFunctionWithNonBooleanFunctions() {
        checkFullTextFunctionsWithNonBooleanFunctions("MATCH", "match(first_name, \"Anna\")", "function");
    }

    public void testTermFunctionWithNonBooleanFunctions() {
        assumeTrue("term function capability not available", EsqlCapabilities.Cap.TERM_FUNCTION.isEnabled());
        checkFullTextFunctionsWithNonBooleanFunctions("Term", "term(first_name, \"Anna\")", "function");
    }

    public void testMatchOperatorWithNonBooleanFunctions() {
        checkFullTextFunctionsWithNonBooleanFunctions(":", "first_name:\"Anna\"", "operator");
    }

    private void checkFullTextFunctionsWithNonBooleanFunctions(String functionName, String functionInvocation, String functionType) {
        if (functionType.equals("operator") == false) {
            // The following tests are only possible for functions from a parsing perspective
            assertEquals(
                "1:19: Invalid condition ["
                    + functionInvocation
                    + " is not null]. ["
                    + functionName
                    + "] "
                    + functionType
                    + " can't be used with ISNOTNULL",
                error("from test | where " + functionInvocation + " is not null")
            );
            assertEquals(
                "1:19: Invalid condition ["
                    + functionInvocation
                    + " is null]. ["
                    + functionName
                    + "] "
                    + functionType
                    + " can't be used with ISNULL",
                error("from test | where " + functionInvocation + " is null")
            );
            assertEquals(
                "1:19: Invalid condition ["
                    + functionInvocation
                    + " in (\"hello\", \"world\")]. ["
                    + functionName
                    + "] "
                    + functionType
                    + " can't be used with IN",
                error("from test | where " + functionInvocation + " in (\"hello\", \"world\")")
            );
        }
        assertEquals(
            "1:19: Invalid condition [coalesce("
                + functionInvocation
                + ", "
                + functionInvocation
                + ")]. ["
                + functionName
                + "] "
                + functionType
                + " can't be used with COALESCE",
            error("from test | where coalesce(" + functionInvocation + ", " + functionInvocation + ")")
        );
        assertEquals(
            "1:19: argument of [concat("
                + functionInvocation
                + ", \"a\")] must be [string], found value ["
                + functionInvocation
                + "] type [boolean]",
            error("from test | where concat(" + functionInvocation + ", \"a\")")
        );
    }

    public void testMatchFunctionArgNotConstant() throws Exception {
        assertEquals(
            "1:19: second argument of [match(first_name, first_name)] must be a constant, received [first_name]",
            error("from test | where match(first_name, first_name)")
        );
        assertEquals(
            "1:59: second argument of [match(first_name, query)] must be a constant, received [query]",
            error("from test | eval query = concat(\"first\", \" name\") | where match(first_name, query)")
        );
        // Other value types are tested in QueryStringFunctionTests
    }

    // These should pass eventually once we lift some restrictions on match function
    public void testMatchFunctionCurrentlyUnsupportedBehaviour() throws Exception {
        assertEquals(
            "1:68: Unknown column [first_name]",
            error("from test | stats max_salary = max(salary) by emp_no | where match(first_name, \"Anna\")")
        );
        assertEquals(
            "1:62: Unknown column [first_name]",
            error("from test | stats max_salary = max(salary) by emp_no | where first_name : \"Anna\"")
        );
    }

    public void testMatchFunctionNullArgs() throws Exception {
        assertEquals(
            "1:19: first argument of [match(null, \"query\")] cannot be null, received [null]",
            error("from test | where match(null, \"query\")")
        );
        assertEquals(
            "1:19: second argument of [match(first_name, null)] cannot be null, received [null]",
            error("from test | where match(first_name, null)")
        );
    }

    public void testMatchTargetsExistingField() throws Exception {
        assertEquals("1:39: Unknown column [first_name]", error("from test | keep emp_no | where match(first_name, \"Anna\")"));
        assertEquals("1:33: Unknown column [first_name]", error("from test | keep emp_no | where first_name : \"Anna\""));
    }

    public void testTermFunctionArgNotConstant() throws Exception {
        assumeTrue("term function capability not available", EsqlCapabilities.Cap.TERM_FUNCTION.isEnabled());
        assertEquals(
            "1:19: second argument of [term(first_name, first_name)] must be a constant, received [first_name]",
            error("from test | where term(first_name, first_name)")
        );
        assertEquals(
            "1:59: second argument of [term(first_name, query)] must be a constant, received [query]",
            error("from test | eval query = concat(\"first\", \" name\") | where term(first_name, query)")
        );
        // Other value types are tested in QueryStringFunctionTests
    }

    // These should pass eventually once we lift some restrictions on match function
    public void testTermFunctionCurrentlyUnsupportedBehaviour() throws Exception {
        assumeTrue("term function capability not available", EsqlCapabilities.Cap.TERM_FUNCTION.isEnabled());
        assertEquals(
            "1:67: Unknown column [first_name]",
            error("from test | stats max_salary = max(salary) by emp_no | where term(first_name, \"Anna\")")
        );
    }

    public void testTermFunctionNullArgs() throws Exception {
        assumeTrue("term function capability not available", EsqlCapabilities.Cap.TERM_FUNCTION.isEnabled());
        assertEquals(
            "1:19: first argument of [term(null, \"query\")] cannot be null, received [null]",
            error("from test | where term(null, \"query\")")
        );
        assertEquals(
            "1:19: second argument of [term(first_name, null)] cannot be null, received [null]",
            error("from test | where term(first_name, null)")
        );
    }

    public void testTermTargetsExistingField() throws Exception {
        assumeTrue("term function capability not available", EsqlCapabilities.Cap.TERM_FUNCTION.isEnabled());
        assertEquals("1:38: Unknown column [first_name]", error("from test | keep emp_no | where term(first_name, \"Anna\")"));
    }

    public void testConditionalFunctionsWithMixedNumericTypes() {
        for (String functionName : List.of("coalesce", "greatest", "least")) {
            assertEquals(
                "1:22: second argument of [" + functionName + "(languages, height)] must be [integer], found value [height] type [double]",
                error("from test | eval x = " + functionName + "(languages, height)")
            );
            assertEquals(
                "1:22: second argument of ["
                    + functionName
                    + "(languages.long, height)] must be [long], found value [height] type [double]",
                error("from test | eval x = " + functionName + "(languages.long, height)")
            );
            assertEquals(
                "1:22: second argument of ["
                    + functionName
                    + "(salary, languages.long)] must be [integer], found value [languages.long] type [long]",
                error("from test | eval x = " + functionName + "(salary, languages.long)")
            );
            assertEquals(
                "1:22: second argument of ["
                    + functionName
                    + "(languages.short, height)] must be [integer], found value [height] type [double]",
                error("from test | eval x = " + functionName + "(languages.short, height)")
            );
            assertEquals(
                "1:22: second argument of ["
                    + functionName
                    + "(languages.byte, height)] must be [integer], found value [height] type [double]",
                error("from test | eval x = " + functionName + "(languages.byte, height)")
            );
            assertEquals(
                "1:22: second argument of ["
                    + functionName
                    + "(languages, height.float)] must be [integer], found value [height.float] type [double]",
                error("from test | eval x = " + functionName + "(languages, height.float)")
            );
            assertEquals(
                "1:22: second argument of ["
                    + functionName
                    + "(languages, height.scaled_float)] must be [integer], "
                    + "found value [height.scaled_float] type [double]",
                error("from test | eval x = " + functionName + "(languages, height.scaled_float)")
            );
            assertEquals(
                "1:22: second argument of ["
                    + functionName
                    + "(languages, height.half_float)] must be [integer], "
                    + "found value [height.half_float] type [double]",
                error("from test | eval x = " + functionName + "(languages, height.half_float)")
            );

            assertEquals(
                "1:22: third argument of ["
                    + functionName
                    + "(null, languages, height)] must be [integer], found value [height] type [double]",
                error("from test | eval x = " + functionName + "(null, languages, height)")
            );
            assertEquals(
                "1:22: third argument of ["
                    + functionName
                    + "(null, languages.long, height)] must be [long], found value [height] type [double]",
                error("from test | eval x = " + functionName + "(null, languages.long, height)")
            );
            assertEquals(
                "1:22: third argument of ["
                    + functionName
                    + "(null, salary, languages.long)] must be [integer], "
                    + "found value [languages.long] type [long]",
                error("from test | eval x = " + functionName + "(null, salary, languages.long)")
            );
            assertEquals(
                "1:22: third argument of ["
                    + functionName
                    + "(null, languages.short, height)] must be [integer], found value [height] type [double]",
                error("from test | eval x = " + functionName + "(null, languages.short, height)")
            );
            assertEquals(
                "1:22: third argument of ["
                    + functionName
                    + "(null, languages.byte, height)] must be [integer], found value [height] type [double]",
                error("from test | eval x = " + functionName + "(null, languages.byte, height)")
            );
            assertEquals(
                "1:22: third argument of ["
                    + functionName
                    + "(null, languages, height.float)] must be [integer], "
                    + "found value [height.float] type [double]",
                error("from test | eval x = " + functionName + "(null, languages, height.float)")
            );
            assertEquals(
                "1:22: third argument of ["
                    + functionName
                    + "(null, languages, height.scaled_float)] must be [integer], "
                    + "found value [height.scaled_float] type [double]",
                error("from test | eval x = " + functionName + "(null, languages, height.scaled_float)")
            );
            assertEquals(
                "1:22: third argument of ["
                    + functionName
                    + "(null, languages, height.half_float)] must be [integer], "
                    + "found value [height.half_float] type [double]",
                error("from test | eval x = " + functionName + "(null, languages, height.half_float)")
            );

            // counter
            assertEquals(
                "1:23: second argument of ["
                    + functionName
                    + "(network.bytes_in, 0)] must be [counter_long], found value [0] type [integer]",
                error("FROM tests | eval x = " + functionName + "(network.bytes_in, 0)", tsdb)
            );

            assertEquals(
                "1:23: second argument of ["
                    + functionName
                    + "(network.bytes_in, to_long(0))] must be [counter_long], "
                    + "found value [to_long(0)] type [long]",
                error("FROM tests | eval x = " + functionName + "(network.bytes_in, to_long(0))", tsdb)
            );
            assertEquals(
                "1:23: second argument of ["
                    + functionName
                    + "(network.bytes_in, 0.0)] must be [counter_long], found value [0.0] type [double]",
                error("FROM tests | eval x = " + functionName + "(network.bytes_in, 0.0)", tsdb)
            );

            assertEquals(
                "1:23: third argument of ["
                    + functionName
                    + "(null, network.bytes_in, 0)] must be [counter_long], found value [0] type [integer]",
                error("FROM tests | eval x = " + functionName + "(null, network.bytes_in, 0)", tsdb)
            );

            assertEquals(
                "1:23: third argument of ["
                    + functionName
                    + "(null, network.bytes_in, to_long(0))] must be [counter_long], "
                    + "found value [to_long(0)] type [long]",
                error("FROM tests | eval x = " + functionName + "(null, network.bytes_in, to_long(0))", tsdb)
            );
            assertEquals(
                "1:23: third argument of ["
                    + functionName
                    + "(null, network.bytes_in, 0.0)] must be [counter_long], found value [0.0] type [double]",
                error("FROM tests | eval x = " + functionName + "(null, network.bytes_in, 0.0)", tsdb)
            );
        }

        // case, a subset tests of coalesce/greatest/least
        assertEquals(
            "1:22: third argument of [case(languages == 1, salary, height)] must be [integer], found value [height] type [double]",
            error("from test | eval x = case(languages == 1, salary, height)")
        );
        assertEquals(
            "1:23: third argument of [case(name == \"a\", network.bytes_in, 0)] must be [counter_long], found value [0] type [integer]",
            error("FROM tests | eval x = case(name == \"a\", network.bytes_in, 0)", tsdb)
        );
    }

    public void testToDatePeriodTimeDurationInInvalidPosition() {
        // arithmetic operations in eval
        assertEquals(
            "1:39: EVAL does not support type [date_period] as the return data type of expression [3 months + 5 days]",
            error("row x = \"2024-01-01\"::datetime | eval y = 3 months + 5 days")
        );

        assertEquals(
            "1:39: EVAL does not support type [date_period] as the return data type of expression "
                + "[\"3 months\"::date_period + \"5 days\"::date_period]",
            error("row x = \"2024-01-01\"::datetime | eval y = \"3 months\"::date_period + \"5 days\"::date_period")
        );

        assertEquals(
            "1:39: EVAL does not support type [time_duration] as the return data type of expression [3 hours + 5 minutes]",
            error("row x = \"2024-01-01\"::datetime | eval y = 3 hours + 5 minutes")
        );

        assertEquals(
            "1:39: EVAL does not support type [time_duration] as the return data type of expression "
                + "[\"3 hours\"::time_duration + \"5 minutes\"::time_duration]",
            error("row x = \"2024-01-01\"::datetime | eval y = \"3 hours\"::time_duration + \"5 minutes\"::time_duration")
        );

        // where
        assertEquals(
            "1:26: first argument of [\"3 days\"::date_period == to_dateperiod(\"3 days\")] must be "
                + "[boolean, cartesian_point, cartesian_shape, date_nanos, datetime, double, geo_point, geo_shape, integer, ip, keyword, "
                + "long, text, unsigned_long or version], found value [\"3 days\"::date_period] type [date_period]",
            error("row x = \"3 days\" | where \"3 days\"::date_period == to_dateperiod(\"3 days\")")
        );

        assertEquals(
            "1:26: first argument of [\"3 hours\"::time_duration <= to_timeduration(\"3 hours\")] must be "
                + "[date_nanos, datetime, double, integer, ip, keyword, long, text, unsigned_long or version], "
                + "found value [\"3 hours\"::time_duration] type [time_duration]",
            error("row x = \"3 days\" | where \"3 hours\"::time_duration <= to_timeduration(\"3 hours\")")
        );

        assertEquals(
            "1:19: second argument of [first_name <= to_timeduration(\"3 hours\")] must be "
                + "[date_nanos, datetime, double, integer, ip, keyword, long, text, unsigned_long or version], "
                + "found value [to_timeduration(\"3 hours\")] type [time_duration]",
            error("from test | where first_name <= to_timeduration(\"3 hours\")")
        );

        assertEquals(
            "1:19: 1st argument of [first_name IN ( to_timeduration(\"3 hours\"), \"3 days\"::date_period)] must be [keyword], "
                + "found value [to_timeduration(\"3 hours\")] type [time_duration]",
            error("from test | where first_name IN ( to_timeduration(\"3 hours\"), \"3 days\"::date_period)")
        );
    }

    public void testToDatePeriodToTimeDurationWithInvalidType() {
        assertEquals(
            "1:36: argument of [1.5::date_period] must be [date_period or string], found value [1.5] type [double]",
            error("from types | EVAL x = birth_date + 1.5::date_period")
        );
        assertEquals(
            "1:37: argument of [to_timeduration(1)] must be [time_duration or string], found value [1] type [integer]",
            error("from types  | EVAL x = birth_date - to_timeduration(1)")
        );
        assertEquals(
            "1:45: argument of [x::date_period] must be [date_period or string], found value [x] type [double]",
            error("from types | EVAL x = 1.5, y = birth_date + x::date_period")
        );
        assertEquals(
            "1:44: argument of [to_timeduration(x)] must be [time_duration or string], found value [x] type [integer]",
            error("from types  | EVAL x = 1, y = birth_date - to_timeduration(x)")
        );
        assertEquals(
            "1:64: argument of [x::date_period] must be [date_period or string], found value [x] type [datetime]",
            error("from types | EVAL x = \"2024-09-08\"::datetime, y = birth_date + x::date_period")
        );
        assertEquals(
            "1:65: argument of [to_timeduration(x)] must be [time_duration or string], found value [x] type [datetime]",
            error("from types  | EVAL x = \"2024-09-08\"::datetime, y = birth_date - to_timeduration(x)")
        );
        assertEquals(
            "1:58: argument of [x::date_period] must be [date_period or string], found value [x] type [ip]",
            error("from types | EVAL x = \"2024-09-08\"::ip, y = birth_date + x::date_period")
        );
        assertEquals(
            "1:59: argument of [to_timeduration(x)] must be [time_duration or string], found value [x] type [ip]",
            error("from types  | EVAL x = \"2024-09-08\"::ip, y = birth_date - to_timeduration(x)")
        );
    }

    public void testIntervalAsString() {
        // DateTrunc
        for (String interval : List.of("1 minu", "1 dy", "1.5 minutes", "0.5 days", "minutes 1", "day 5")) {
            assertThat(
                error("from types  | EVAL x = date_trunc(\"" + interval + "\", \"1991-06-26T00:00:00.000Z\")"),
                containsString("1:35: Cannot convert string [" + interval + "] to [DATE_PERIOD or TIME_DURATION]")
            );
            assertThat(
                error("from types  | EVAL x = \"1991-06-26T00:00:00.000Z\", y = date_trunc(\"" + interval + "\", x::datetime)"),
                containsString("1:67: Cannot convert string [" + interval + "] to [DATE_PERIOD or TIME_DURATION]")
            );
        }
        for (String interval : List.of("1", "0.5", "invalid")) {
            assertThat(
                error("from types  | EVAL x = date_trunc(\"" + interval + "\", \"1991-06-26T00:00:00.000Z\")"),
                containsString(
                    "1:24: first argument of [date_trunc(\""
                        + interval
                        + "\", \"1991-06-26T00:00:00.000Z\")] must be [dateperiod or timeduration], found value [\""
                        + interval
                        + "\"] type [keyword]"
                )
            );
            assertThat(
                error("from types  | EVAL x = \"1991-06-26T00:00:00.000Z\", y = date_trunc(\"" + interval + "\", x::datetime)"),
                containsString(
                    "1:56: first argument of [date_trunc(\""
                        + interval
                        + "\", x::datetime)] "
                        + "must be [dateperiod or timeduration], found value [\""
                        + interval
                        + "\"] type [keyword]"
                )
            );
        }

        // Bucket
        assertEquals(
            "1:52: Cannot convert string [1 yar] to [DATE_PERIOD or TIME_DURATION], error [Unexpected temporal unit: 'yar']",
            error("from test | stats max(emp_no) by bucket(hire_date, \"1 yar\")")
        );
        assertEquals(
            "1:52: Cannot convert string [1 hur] to [DATE_PERIOD or TIME_DURATION], error [Unexpected temporal unit: 'hur']",
            error("from test | stats max(emp_no) by bucket(hire_date, \"1 hur\")")
        );
        assertEquals(
            "1:58: Cannot convert string [1 mu] to [DATE_PERIOD or TIME_DURATION], error [Unexpected temporal unit: 'mu']",
            error("from test | stats max = max(emp_no) by bucket(hire_date, \"1 mu\") | sort max ")
        );
        assertEquals(
            "1:34: second argument of [bucket(hire_date, \"1\")] must be [integral, date_period or time_duration], "
                + "found value [\"1\"] type [keyword]",
            error("from test | stats max(emp_no) by bucket(hire_date, \"1\")")
        );
        assertEquals(
            "1:40: second argument of [bucket(hire_date, \"1\")] must be [integral, date_period or time_duration], "
                + "found value [\"1\"] type [keyword]",
            error("from test | stats max = max(emp_no) by bucket(hire_date, \"1\") | sort max ")
        );
        assertEquals(
            "1:68: second argument of [bucket(y, \"1\")] must be [integral, date_period or time_duration], "
                + "found value [\"1\"] type [keyword]",
            error("from test | eval x = emp_no, y = hire_date | stats max = max(x) by bucket(y, \"1\") | sort max ")
        );
    }

    public void testCategorizeOnlyFirstGrouping() {
        query("FROM test | STATS COUNT(*) BY CATEGORIZE(first_name)");
        query("FROM test | STATS COUNT(*) BY cat = CATEGORIZE(first_name)");
        query("FROM test | STATS COUNT(*) BY CATEGORIZE(first_name), emp_no");
        query("FROM test | STATS COUNT(*) BY a = CATEGORIZE(first_name), b = emp_no");

        assertEquals(
            "1:39: CATEGORIZE grouping function [CATEGORIZE(first_name)] can only be in the first grouping expression",
            error("FROM test | STATS COUNT(*) BY emp_no, CATEGORIZE(first_name)")
        );
        assertEquals(
            "1:55: CATEGORIZE grouping function [CATEGORIZE(last_name)] can only be in the first grouping expression",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(first_name), CATEGORIZE(last_name)")
        );
        assertEquals(
            "1:55: CATEGORIZE grouping function [CATEGORIZE(first_name)] can only be in the first grouping expression",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(first_name), CATEGORIZE(first_name)")
        );
        assertEquals(
            "1:63: CATEGORIZE grouping function [CATEGORIZE(last_name)] can only be in the first grouping expression",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(first_name), emp_no, CATEGORIZE(last_name)")
        );
        assertEquals(
            "1:63: CATEGORIZE grouping function [CATEGORIZE(first_name)] can only be in the first grouping expression",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(first_name), emp_no, CATEGORIZE(first_name)")
        );
    }

    public void testCategorizeNestedGrouping() {
        query("from test | STATS COUNT(*) BY CATEGORIZE(LENGTH(first_name)::string)");

        assertEquals(
            "1:40: CATEGORIZE grouping function [CATEGORIZE(first_name)] can't be used within other expressions",
            error("FROM test | STATS COUNT(*) BY MV_COUNT(CATEGORIZE(first_name))")
        );
        assertEquals(
            "1:31: CATEGORIZE grouping function [CATEGORIZE(first_name)] can't be used within other expressions",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(first_name)::datetime")
        );
    }

    public void testCategorizeWithinAggregations() {
        query("from test | STATS MV_COUNT(cat), COUNT(*) BY cat = CATEGORIZE(first_name)");
        query("from test | STATS MV_COUNT(CATEGORIZE(first_name)), COUNT(*) BY cat = CATEGORIZE(first_name)");
        query("from test | STATS MV_COUNT(CATEGORIZE(first_name)), COUNT(*) BY CATEGORIZE(first_name)");

        assertEquals(
            "1:25: cannot use CATEGORIZE grouping function [CATEGORIZE(first_name)] within an aggregation",
            error("FROM test | STATS COUNT(CATEGORIZE(first_name)) BY CATEGORIZE(first_name)")
        );
        assertEquals(
            "1:25: cannot reference CATEGORIZE grouping function [cat] within an aggregation",
            error("FROM test | STATS COUNT(cat) BY cat = CATEGORIZE(first_name)")
        );
        assertEquals(
            "1:30: cannot reference CATEGORIZE grouping function [cat] within an aggregation",
            error("FROM test | STATS SUM(LENGTH(cat::keyword) + LENGTH(last_name)) BY cat = CATEGORIZE(first_name)")
        );
        assertEquals(
            "1:25: cannot reference CATEGORIZE grouping function [`CATEGORIZE(first_name)`] within an aggregation",
            error("FROM test | STATS COUNT(`CATEGORIZE(first_name)`) BY CATEGORIZE(first_name)")
        );

        assertEquals(
            "1:28: can only use grouping function [CATEGORIZE(last_name)] as part of the BY clause",
            error("FROM test | STATS MV_COUNT(CATEGORIZE(last_name)) BY CATEGORIZE(first_name)")
        );
    }

    public void testCategorizeWithFilteredAggregations() {
        query("FROM test | STATS COUNT(*) WHERE first_name == \"John\" BY CATEGORIZE(last_name)");
        query("FROM test | STATS COUNT(*) WHERE last_name == \"Doe\" BY CATEGORIZE(last_name)");

        assertEquals(
            "1:34: can only use grouping function [CATEGORIZE(first_name)] as part of the BY clause",
            error("FROM test | STATS COUNT(*) WHERE CATEGORIZE(first_name) == \"John\" BY CATEGORIZE(last_name)")
        );
        assertEquals(
            "1:34: can only use grouping function [CATEGORIZE(last_name)] as part of the BY clause",
            error("FROM test | STATS COUNT(*) WHERE CATEGORIZE(last_name) == \"Doe\" BY CATEGORIZE(last_name)")
        );
        assertEquals(
            "1:34: cannot reference CATEGORIZE grouping function [category] within an aggregation filter",
            error("FROM test | STATS COUNT(*) WHERE category == \"Doe\" BY category = CATEGORIZE(last_name)")
        );
    }

    public void testChangePoint() {
        assumeTrue("change_point must be enabled", EsqlCapabilities.Cap.CHANGE_POINT.isEnabled());
        var airports = AnalyzerTestUtils.analyzer(loadMapping("mapping-airports.json", "airports"));
        assertEquals("1:30: Unknown column [blahblah]", error("FROM airports | CHANGE_POINT blahblah ON scalerank", airports));
        assertEquals("1:43: Unknown column [blahblah]", error("FROM airports | CHANGE_POINT scalerank ON blahblah", airports));
        // TODO: nicer error message for missing default column "@timestamp"
        assertEquals("1:17: Unknown column [@timestamp]", error("FROM airports | CHANGE_POINT scalerank", airports));
    }

    public void testChangePoint_keySortable() {
        assumeTrue("change_point must be enabled", EsqlCapabilities.Cap.CHANGE_POINT.isEnabled());
        List<DataType> sortableTypes = List.of(BOOLEAN, DOUBLE, DATE_NANOS, DATETIME, INTEGER, IP, KEYWORD, LONG, UNSIGNED_LONG, VERSION);
        List<DataType> unsortableTypes = List.of(CARTESIAN_POINT, CARTESIAN_SHAPE, GEO_POINT, GEO_SHAPE);
        for (DataType type : sortableTypes) {
            query(Strings.format("ROW key=NULL::%s, value=0\n | CHANGE_POINT value ON key", type));
        }
        for (DataType type : unsortableTypes) {
            assertEquals(
                "2:4: change point key [key] must be sortable",
                error(Strings.format("ROW key=NULL::%s, value=0\n | CHANGE_POINT value ON key", type))
            );
        }
    }

    public void testChangePoint_valueNumeric() {
        assumeTrue("change_point must be enabled", EsqlCapabilities.Cap.CHANGE_POINT.isEnabled());
        List<DataType> numericTypes = List.of(DOUBLE, INTEGER, LONG, UNSIGNED_LONG);
        List<DataType> nonNumericTypes = List.of(
            BOOLEAN,
            CARTESIAN_POINT,
            CARTESIAN_SHAPE,
            DATE_NANOS,
            DATETIME,
            GEO_POINT,
            GEO_SHAPE,
            IP,
            KEYWORD,
            VERSION
        );
        for (DataType type : numericTypes) {
            query(Strings.format("ROW key=0, value=NULL::%s\n | CHANGE_POINT value ON key", type));
        }
        for (DataType type : nonNumericTypes) {
            assertEquals(
                "2:4: change point value [value] must be numeric",
                error(Strings.format("ROW key=0, value=NULL::%s\n | CHANGE_POINT value ON key", type))
            );
        }
        assertEquals("2:4: change point value [value] must be numeric", error("ROW key=0, value=NULL\n | CHANGE_POINT value ON key"));
    }

    public void testSortByAggregate() {
        assertEquals("1:18: aggregate function [count(*)] not allowed outside STATS command", error("ROW a = 1 | SORT count(*)"));
        assertEquals(
            "1:28: aggregate function [count(*)] not allowed outside STATS command",
            error("ROW a = 1 | SORT to_string(count(*))")
        );
        assertEquals("1:22: aggregate function [max(a)] not allowed outside STATS command", error("ROW a = 1 | SORT 1 + max(a)"));
        assertEquals("1:18: aggregate function [count(*)] not allowed outside STATS command", error("FROM test | SORT count(*)"));
    }

    public void testFilterByAggregate() {
        assertEquals("1:19: aggregate function [count(*)] not allowed outside STATS command", error("ROW a = 1 | WHERE count(*) > 0"));
        assertEquals(
            "1:29: aggregate function [count(*)] not allowed outside STATS command",
            error("ROW a = 1 | WHERE to_string(count(*)) IS NOT NULL")
        );
        assertEquals("1:23: aggregate function [max(a)] not allowed outside STATS command", error("ROW a = 1 | WHERE 1 + max(a) > 0"));
        assertEquals(
            "1:24: aggregate function [min(languages)] not allowed outside STATS command",
            error("FROM employees | WHERE min(languages) > 2")
        );
    }

    public void testDissectByAggregate() {
        assertEquals(
            "1:21: aggregate function [min(first_name)] not allowed outside STATS command",
            error("from test | dissect min(first_name) \"%{foo}\"")
        );
        assertEquals(
            "1:21: aggregate function [avg(salary)] not allowed outside STATS command",
            error("from test | dissect avg(salary) \"%{foo}\"")
        );
    }

    public void testGrokByAggregate() {
        assertEquals(
            "1:18: aggregate function [max(last_name)] not allowed outside STATS command",
            error("from test | grok max(last_name) \"%{WORD:foo}\"")
        );
        assertEquals(
            "1:18: aggregate function [sum(salary)] not allowed outside STATS command",
            error("from test | grok sum(salary) \"%{WORD:foo}\"")
        );
    }

    public void testAggregateInRow() {
        assertEquals("1:13: aggregate function [count(*)] not allowed outside STATS command", error("ROW a = 1 + count(*)"));
        assertEquals("1:9: aggregate function [avg(2)] not allowed outside STATS command", error("ROW a = avg(2)"));
    }

    public void testLookupJoinDataTypeMismatch() {
        assumeTrue("requires LOOKUP JOIN capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());

        query("FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code");

        assertEquals(
            "1:87: JOIN left field [language_code] of type [KEYWORD] is incompatible with right field [language_code] of type [INTEGER]",
            error("FROM test | EVAL language_code = languages::keyword | LOOKUP JOIN languages_lookup ON language_code")
        );
    }

    public void testMatchOptions() {
        // Check positive cases
        query("FROM test | WHERE match(first_name, \"Jean\", {\"analyzer\": \"standard\"})");
        query("FROM test | WHERE match(first_name, \"Jean\", {\"boost\": 2.1})");
        query("FROM test | WHERE match(first_name, \"Jean\", {\"fuzziness\": 2})");
        query("FROM test | WHERE match(first_name, \"Jean\", {\"fuzziness\": \"AUTO\"})");
        query("FROM test | WHERE match(first_name, \"Jean\", {\"fuzzy_transpositions\": false})");
        query("FROM test | WHERE match(first_name, \"Jean\", {\"lenient\": false})");
        query("FROM test | WHERE match(first_name, \"Jean\", {\"max_expansions\": 10})");
        query("FROM test | WHERE match(first_name, \"Jean\", {\"minimum_should_match\": \"2\"})");
        query("FROM test | WHERE match(first_name, \"Jean\", {\"operator\": \"AND\"})");
        query("FROM test | WHERE match(first_name, \"Jean\", {\"prefix_length\": 2})");
        query("FROM test | WHERE match(first_name, \"Jean\", {\"auto_generate_synonyms_phrase_query\": true})");

        // Check all data types for available options
        DataType[] optionTypes = new DataType[] { INTEGER, LONG, FLOAT, DOUBLE, KEYWORD, BOOLEAN };
        for (Map.Entry<String, DataType> allowedOptions : Match.ALLOWED_OPTIONS.entrySet()) {
            String optionName = allowedOptions.getKey();
            DataType optionType = allowedOptions.getValue();
            // Check every possible type for the option - we'll try to convert it to the expected type
            for (DataType currentType : optionTypes) {
                String optionValue = switch (currentType) {
                    case BOOLEAN -> String.valueOf(randomBoolean());
                    case INTEGER -> String.valueOf(randomIntBetween(0, 100000));
                    case LONG -> String.valueOf(randomLong());
                    case FLOAT -> String.valueOf(randomFloat());
                    case DOUBLE -> String.valueOf(randomDouble());
                    case KEYWORD -> randomAlphaOfLength(10);
                    default -> throw new IllegalArgumentException("Unsupported option type: " + currentType);
                };
                String queryOptionValue = optionValue;
                if (currentType == KEYWORD) {
                    queryOptionValue = "\"" + optionValue + "\"";
                }

                String query = "FROM test | WHERE match(first_name, \"Jean\", {\"" + optionName + "\": " + queryOptionValue + "})";
                try {
                    // Check conversion is possible
                    DataTypeConverter.convert(optionValue, optionType);
                    // If no exception was thrown, conversion is possible and should be done
                    query(query);
                } catch (InvalidArgumentException e) {
                    // Conversion is not possible, query should fail
                    assertEquals(
                        "1:19: Invalid option ["
                            + optionName
                            + "] in [match(first_name, \"Jean\", {\""
                            + optionName
                            + "\": "
                            + queryOptionValue
                            + "})], cannot cast ["
                            + optionValue
                            + "] to ["
                            + optionType.typeName()
                            + "]",
                        error(query)
                    );
                }
            }
        }

        assertThat(
            error("FROM test | WHERE match(first_name, \"Jean\", {\"unknown_option\": true})"),
            containsString(
                "1:19: Invalid option [unknown_option] in [match(first_name, \"Jean\", {\"unknown_option\": true})]," + " expected one of "
            )
        );
    }

    public void testQueryStringOptions() {
        // Check positive cases
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"analyzer\": \"standard\"})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"allow_leading_wildcard\": false})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"analyze_wildcard\": false})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"auto_generate_synonyms_phrase_query\": true})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"boost\": 2.1})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"default_field\": \"field1\"})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"default_operator\": \"AND\"})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"enable_position_increments\": false})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"fuzziness\": 2})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"fuzziness\": \"AUTO\"})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"fuzzy_prefix_length\": 5})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"fuzzy_transpositions\": false})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"lenient\": false})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"max_determinized_states\": 10})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"minimum_should_match\": \"2\"})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"quote_analyzer\": \"qnalyzer_1\"})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"quote_field_suffix\": \"q_suffix\"})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"phrase_slop\": 10})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"rewrite\": \"r1\"})");
        query("FROM test | WHERE QSTR(\"first_name: Jean\", {\"time_zone\": \"time_zone\"})");

        // Check all data types for available options
        DataType[] optionTypes = new DataType[] { INTEGER, LONG, FLOAT, DOUBLE, KEYWORD, BOOLEAN };
        for (Map.Entry<String, DataType> allowedOptions : QueryString.ALLOWED_OPTIONS.entrySet()) {
            String optionName = allowedOptions.getKey();
            DataType optionType = allowedOptions.getValue();
            // Check every possible type for the option - we'll try to convert it to the expected type
            for (DataType currentType : optionTypes) {
                String optionValue = switch (currentType) {
                    case BOOLEAN -> String.valueOf(randomBoolean());
                    case INTEGER -> String.valueOf(randomIntBetween(0, 100000));
                    case LONG -> String.valueOf(randomLong());
                    case FLOAT -> String.valueOf(randomFloat());
                    case DOUBLE -> String.valueOf(randomDouble());
                    case KEYWORD -> randomAlphaOfLength(10);
                    default -> throw new IllegalArgumentException("Unsupported option type: " + currentType);
                };
                String queryOptionValue = optionValue;
                if (currentType == KEYWORD) {
                    queryOptionValue = "\"" + optionValue + "\"";
                }

                String query = "FROM test | WHERE QSTR(\"first_name: Jean\", {\"" + optionName + "\": " + queryOptionValue + "})";
                try {
                    // Check conversion is possible
                    DataTypeConverter.convert(optionValue, optionType);
                    // If no exception was thrown, conversion is possible and should be done
                    query(query);
                } catch (InvalidArgumentException e) {
                    // Conversion is not possible, query should fail
                    assertEquals(
                        "1:19: Invalid option ["
                            + optionName
                            + "] in [QSTR(\"first_name: Jean\", {\""
                            + optionName
                            + "\": "
                            + queryOptionValue
                            + "})], cannot cast ["
                            + optionValue
                            + "] to ["
                            + optionType.typeName()
                            + "]",
                        error(query)
                    );
                }
            }
        }

        assertThat(
            error("FROM test |  WHERE QSTR(\"first_name: Jean\", {\"unknown_option\": true})"),
            containsString(
                "1:20: Invalid option [unknown_option] in [QSTR(\"first_name: Jean\", {\"unknown_option\": true})]," + " expected one of "
            )
        );
    }

    public void testMultiMatchOptions() {
        // Check positive cases
        query("FROM test | WHERE MULTI_MATCH(first_name, \"Jean\")");
        query("FROM test | WHERE MULTI_MATCH(first_name, \"Jean\", {\"analyzer\": \"standard\"})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"analyzer\": \"standard\"})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"slop\": 10})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"auto_generate_synonyms_phrase_query\": true})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"fuzziness\": 2})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"fuzzy_transpositions\": false})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"lenient\": false})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"max_expansions\": 10})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"minimum_should_match\": \"2\"})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"operator\": \"AND\"})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"prefix_length\": 2})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"tie_breaker\": 1.0})");
        query("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"type\": \"best_fields\"})");

        // Check all data types for available options
        DataType[] optionTypes = new DataType[] { INTEGER, LONG, FLOAT, DOUBLE, KEYWORD, BOOLEAN };
        for (Map.Entry<String, DataType> allowedOptions : MultiMatch.OPTIONS.entrySet()) {
            String optionName = allowedOptions.getKey();
            DataType optionType = allowedOptions.getValue();
            // Check every possible type for the option - we'll try to convert it to the expected type
            for (DataType currentType : optionTypes) {
                String optionValue = switch (currentType) {
                    case BOOLEAN -> String.valueOf(randomBoolean());
                    case INTEGER -> String.valueOf(randomIntBetween(0, 100000));
                    case LONG -> String.valueOf(randomLong());
                    case FLOAT -> String.valueOf(randomFloat());
                    case DOUBLE -> String.valueOf(randomDouble());
                    case KEYWORD -> randomAlphaOfLength(10);
                    default -> throw new IllegalArgumentException("Unsupported option type: " + currentType);
                };
                String queryOptionValue = optionValue;
                if (currentType == KEYWORD) {
                    queryOptionValue = "\"" + optionValue + "\"";
                }

                String query = "FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\""
                    + optionName
                    + "\": "
                    + queryOptionValue
                    + "})";
                try {
                    // Check conversion is possible
                    DataTypeConverter.convert(optionValue, optionType);
                    // If no exception was thrown, conversion is possible and should be done
                    query(query);
                } catch (InvalidArgumentException e) {
                    // Conversion is not possible, query should fail
                    assertEquals(
                        "1:19: Invalid option ["
                            + optionName
                            + "] in [MULTI_MATCH(first_name, last_name, \"Jean\", {\""
                            + optionName
                            + "\": "
                            + queryOptionValue
                            + "})], cannot "
                            + (optionType == OBJECT ? "convert from" : "cast")
                            + " ["
                            + optionValue
                            + "]"
                            + (optionType == OBJECT ? (", type [keyword]") : "")
                            + " to ["
                            + optionType.typeName()
                            + "]",
                        error(query)
                    );
                }
            }
        }

        assertThat(
            error("FROM test | WHERE MULTI_MATCH(first_name, last_name, \"Jean\", {\"unknown_option\": true})"),
            containsString(
                "1:19: Invalid option [unknown_option] in [MULTI_MATCH(first_name, last_name, \"Jean\", "
                    + "{\"unknown_option\": true})], expected one of "
            )
        );
    }

    public void testMultiMatchFunctionIsNotNullable() {
        assertEquals(
            "1:54: [MultiMatch] function cannot operate on [text::keyword], which is not a field from an index mapping",
            error("row n = null | eval text = n + 5 | where multi_match(text::keyword, \"Anna\")")
        );
    }

    public void testMultiMatchWithNonIndexedColumnCurrentlyUnsupported() {
        assertEquals(
            "1:73: [MultiMatch] function cannot operate on [initial], which is not a field from an index mapping",
            error("from test | eval initial = substring(first_name, 1) | where multi_match(initial, \"A\")")
        );
        assertEquals(
            "1:73: [MultiMatch] function cannot operate on [text], which is not a field from an index mapping",
            error("from test | eval text=concat(first_name, last_name) | where multi_match(text, \"cat\")")
        );
    }

    public void testMultiMatchFunctionNotAllowedAfterCommands() throws Exception {
        assertEquals(
            "1:24: [MultiMatch] function cannot be used after LIMIT",
            error("from test | limit 10 | where multi_match(first_name, \"Anna\")")
        );
        assertEquals(
            "1:47: [MultiMatch] function cannot be used after STATS",
            error("from test | STATS c = AVG(salary) BY gender | where multi_match(gender, \"F\")")
        );
    }

    public void testMultiMatchFunctionWithDisjunctions() {
        checkWithDisjunctions("MultiMatch", "multi_match(first_name, last_name, \"Anna\")", "function");
    }

    public void testMultiMatchFunctionWithNonBooleanFunctions() {
        checkFullTextFunctionsWithNonBooleanFunctions("MultiMatch", "multi_match(first_name, last_name, \"Anna\")", "function");
    }

    public void testMultiMatchFunctionArgNotConstant() throws Exception {
        assertEquals(
            "1:19: second argument of [match(first_name, first_name)] must be a constant, received [first_name]",
            error("from test | where match(first_name, first_name)")
        );
        assertEquals(
            "1:59: second argument of [match(first_name, query)] must be a constant, received [query]",
            error("from test | eval query = concat(\"first\", \" name\") | where match(first_name, query)")
        );
        // Other value types are tested in QueryStringFunctionTests
    }

    // Should pass eventually once we lift some restrictions on the multi-match function.
    public void testMultiMatchFunctionCurrentlyUnsupportedBehaviour() throws Exception {
        assertEquals(
            "1:74: Unknown column [first_name]\nline 1:86: Unknown column [last_name]",
            error("from test | stats max_salary = max(salary) by emp_no | where multi_match(first_name, last_name, \"Anna\")")
        );
    }

    public void testMultiMatchFunctionNullArgs() throws Exception {
        assertEquals(
            "1:19: second argument of [multi_match(\"query\", null)] cannot be null, received [null]",
            error("from test | where multi_match(\"query\", null)")
        );
        assertEquals(
            "1:19: second argument of [multi_match(first_name, null)] cannot be null, received [null]",
            error("from test | where multi_match(first_name, null)")
        );
    }

    public void testMultiMatchTargetsExistingField() throws Exception {
        assertEquals(
            "1:45: Unknown column [first_name]\nline 1:57: Unknown column [last_name]",
            error("from test | keep emp_no | where multi_match(first_name, last_name, \"Anna\")")
        );
    }

    public void testMultiMatchInsideEval() throws Exception {
        assumeTrue("MultiMatch operator is available just for snapshots", Build.current().isSnapshot());
        assertEquals(
            "1:36: [MultiMatch] function is only supported in WHERE and STATS commands\n"
                + "line 1:48: [MultiMatch] function cannot operate on [title], which is not a field from an index mapping",
            error("row title = \"brown fox\" | eval x = multi_match(title, \"fox\")")
        );
    }

    public void testInsistNotOnTopOfFrom() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        assertThat(
            error("FROM test | EVAL foo = 42 | INSIST_🐔 bar"),
            containsString("1:29: [insist] can only be used after [from] or [insist] commands, but was [EVAL foo = 42]")
        );
    }

    public void testFullTextFunctionsInStats() {
        checkFullTextFunctionsInStats("match(last_name, \"Smith\")");
        checkFullTextFunctionsInStats("multi_match(first_name, last_name, \"Smith\")");
        checkFullTextFunctionsInStats("last_name : \"Smith\"");
        checkFullTextFunctionsInStats("qstr(\"last_name: Smith\")");
        checkFullTextFunctionsInStats("kql(\"last_name: Smith\")");
    }

    private void checkFullTextFunctionsInStats(String functionInvocation) {

        query("from test | stats c = max(salary) where " + functionInvocation);
        query("from test | stats c = max(salary) where " + functionInvocation + " or length(first_name) > 10");
        query("from test metadata _score |  where " + functionInvocation + " | stats c = max(_score)");
        query("from test metadata _score |  where " + functionInvocation + " or length(first_name) > 10 | stats c = max(_score)");

        assertThat(
            error("from test metadata _score | stats c = max(_score) where " + functionInvocation),
            containsString("cannot use _score aggregations with a WHERE filter in a STATS command")
        );
    }

    private void query(String query) {
        query(query, defaultAnalyzer);
    }

    private void query(String query, Analyzer analyzer) {
        analyzer.analyze(parser.createStatement(query));
    }

    private String error(String query) {
        return error(query, defaultAnalyzer);
    }

    private String error(String query, Object... params) {
        return error(query, defaultAnalyzer, params);
    }

    private String error(String query, Analyzer analyzer, Object... params) {
        return error(query, analyzer, VerificationException.class, params);
    }

    private String error(String query, Analyzer analyzer, Class<? extends Exception> exception, Object... params) {
        List<QueryParam> parameters = new ArrayList<>();
        for (Object param : params) {
            if (param == null) {
                parameters.add(paramAsConstant(null, null));
            } else if (param instanceof String) {
                parameters.add(paramAsConstant(null, param));
            } else if (param instanceof Number) {
                parameters.add(paramAsConstant(null, param));
            } else {
                throw new IllegalArgumentException("VerifierTests don't support params of type " + param.getClass());
            }
        }
        Throwable e = expectThrows(
            exception,
            "Expected error for query [" + query + "] but no error was raised",
            () -> analyzer.analyze(parser.createStatement(query, new QueryParams(parameters)))
        );
        assertThat(e, instanceOf(exception));

        String message = e.getMessage();
        if (e instanceof VerificationException) {
            assertTrue(message.startsWith("Found "));
        }
        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
