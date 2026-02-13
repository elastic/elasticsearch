/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MultiMatch;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.IndexPattern;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.TEXT_EMBEDDING_INFERENCE_ID;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
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
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOTILE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.startsWith;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
/**
 * Parses a plan, builds an AST for it, runs logical analysis and post analysis verification.
 * So if we don't error out in the process, post analysis verification passed
 * Use this class if you want to test post analysis verification
 * and especially if you expect to get a VerificationException
 */
public class VerifierTests extends ESTestCase {

    private final Analyzer defaultAnalyzer = AnalyzerTestUtils.expandedDefaultAnalyzer();
    private final Analyzer fullTextAnalyzer = AnalyzerTestUtils.analyzer(loadMapping("mapping-full_text_search.json", "test"));
    private final Analyzer sampleDataAnalyzer = AnalyzerTestUtils.analyzer(loadMapping("mapping-sample_data.json", "test"));
    private final Analyzer oddSampleDataAnalyzer = AnalyzerTestUtils.analyzer(loadMapping("mapping-odd-timestamp.json", "test"));
    private final Analyzer tsdb = AnalyzerTestUtils.analyzer(AnalyzerTestUtils.tsdbIndexResolution());
    private Analyzer k8s;
    {
        // Load Time Series mappings for these tests
        Map<String, EsField> mappingK8s = EsqlTestUtils.loadMapping("k8s-mappings.json");
        EsIndex k8sIndex = EsIndexGenerator.esIndex("k8s", mappingK8s, Map.of("k8s", IndexMode.TIME_SERIES));
        IndexResolution getIndexResult = IndexResolution.valid(k8sIndex);
        k8s = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                Map.of(new IndexPattern(Source.EMPTY, "k8s"), getIndexResult),
                defaultLookupResolution(),
                new EnrichResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
    }
    private final List<String> TIME_DURATIONS = List.of("millisecond", "second", "minute", "hour");
    private final List<String> DATE_PERIODS = List.of("day", "week", "month", "year");

    public void testIncompatibleTypesInMathOperation() {
        assertEquals(
            "1:40: second argument of [a + c] must be [date_nanos, datetime, numeric or dense_vector], found value [c] type [keyword]",
            error("row a = 1, b = 2, c = \"xxx\" | eval y = a + c")
        );
        assertEquals(
            "1:40: second argument of [a - c] must be [date_nanos, datetime, numeric or dense_vector], found value [c] type [keyword]",
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
            EsIndexGenerator.esIndex(
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
        if (EsqlCapabilities.Cap.INLINE_STATS.isEnabled()) {
            assertEquals(
                "1:39: Cannot use field [unsupported] with unsupported type [flattened]",
                error("from test* | inline stats count(1) by unsupported", analyzer)
            );
            assertEquals(
                "1:39: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                    + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
                error("from test* | inline stats count(1) by multi_typed", analyzer)
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
        if (EsqlCapabilities.Cap.INLINE_STATS.isEnabled()) {
            assertEquals(
                "1:34: Cannot use field [unsupported] with unsupported type [flattened]",
                error("from test* | inline stats values(unsupported)", analyzer)
            );
            assertEquals(
                "1:34: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                    + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
                error("from test* | inline stats values(multi_typed)", analyzer)
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

        // Verify that UnsupportedAttribute can pass through KEEP (Project) unchanged without error.
        // This is valid because the field is just being projected, not used in operations.
        query("from test* | keep unsupported", analyzer);
        query("from test* | keep multi_typed", analyzer);

        // Verify that renaming UnsupportedAttribute fails even after passing through KEEP.
        // This validates the fix for EsqlProject consolidation: the rename check runs unconditionally,
        // not gated by Project.expressionsResolved() which treats UnsupportedAttribute as resolved.
        assertEquals(
            "1:40: Cannot use field [unsupported] with unsupported type [flattened]",
            error("from test* | keep unsupported | rename unsupported as x", analyzer)
        );
        assertEquals(
            "1:40: Cannot use field [multi_typed] due to ambiguities being mapped as [2] incompatible types:"
                + " [ip] in [test1, test2, test3] and [2] other indices, [keyword] in [test6]",
            error("from test* | keep multi_typed | rename multi_typed as x", analyzer)
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

        assertEquals(
            "1:76: cannot use [double] as an input of FUSE. Consider using [DROP double] before FUSE.",
            error("from test* METADATA _id, _index, _score | FORK (where true) (where true) | FUSE", analyzer)
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
            "1:25: argument of [avg(first_name)] must be [aggregate_metric_double,"
                + " exponential_histogram, tdigest or numeric except unsigned_long or counter types],"
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
        if (EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled()) {
            assertEquals(
                "1:19: grouping key [[q].[e]] already specified in the STATS BY clause",
                error("from test | stats [q].[e] BY [q].[e]")
            );
            assertEquals(
                "1:19: Cannot specify grouping expression [[q].[e]] as an aggregate",
                error("from test | stats [q].[e] BY x = [q].[e]")
            );
        }

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

    public void testDropUnknownPattern() {
        assertEquals("1:18: No matches found for pattern [foobar*]", error("from test | drop foobar*"));
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
            "1:19: argument of [sum(hire_date)] must be [aggregate_metric_double,"
                + " exponential_histogram, tdigest or numeric except unsigned_long or counter types],"
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
        query("from test | EVAL x = null | where x");

        // `to_string(null)` is of `KEYWORD` type null, resulting in `to_string(null) == "abc"` being of `BOOLEAN`
        query("from test | where to_string(null) == \"abc\"");

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
        for (String grid : new String[] { "geohash", "geotile", "geohex" }) {
            String gridFunc = "ST_" + grid.toUpperCase(Locale.ROOT);
            String literalQuery = prefix + "| EVAL grid = " + gridFunc + "(TO_GEOPOINT(wkt),1) | limit 5 | sort grid";
            String indexQuery = "FROM airports | LIMIT 5 | EVAL grid = " + gridFunc + "(location, 1) | sort grid";
            String literalError = "1:" + (136 + grid.length()) + ": cannot sort on " + grid;
            String indexError = "1:" + (63 + grid.length()) + ": cannot sort on " + grid;
            if (EsqlCapabilities.Cap.SPATIAL_GRID_TYPES.isEnabled() == false) {
                literalError = "1:95: Unknown function [" + gridFunc + "]";
                indexError = "1:39: Unknown function [" + gridFunc + "]";
            }
            assertThat(grid, error(literalQuery), startsWith(literalError));
            assertThat(grid, error(indexQuery, airports), startsWith(indexError));
        }
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
            error("FROM test | STATS min(network.bytes_in)", tsdb),
            equalTo(
                "1:19: argument of [min(network.bytes_in)] must be"
                    + " [boolean, date, ip, string, version, aggregate_metric_double,"
                    + " exponential_histogram, tdigest or numeric except counter types],"
                    + " found value [network.bytes_in] type [counter_long]"
            )
        );

        assertThat(
            error("FROM test | STATS max(network.bytes_in)", tsdb),
            equalTo(
                "1:19: argument of [max(network.bytes_in)] must be"
                    + " [boolean, date, ip, string, version, aggregate_metric_double, exponential_histogram,"
                    + " tdigest or numeric except counter types],"
                    + " found value [network.bytes_in] type [counter_long]"
            )
        );

        assertThat(
            error("FROM test | STATS count(network.bytes_out)", tsdb),
            equalTo(
                "1:19: argument of [count(network.bytes_out)] must be"
                    + " [any type except counter types, histogram, or date_range],"
                    + " found value [network.bytes_out] type [counter_long]"
            )
        );
    }

    public void testGroupByCounter() {
        assertThat(
            error("FROM test | STATS count(*) BY network.bytes_in", tsdb),
            equalTo("1:31: cannot group by on [counter_long] type for grouping [network.bytes_in]")
        );

        assertThat(
            error("FROM test | STATS present(name) BY network.bytes_in", tsdb),
            equalTo("1:36: cannot group by on [counter_long] type for grouping [network.bytes_in]")
        );
    }

    public void testRenameOrDropTimestmapWithRate() {
        assertThat(
            error("TS k8s | RENAME @timestamp AS newTs | STATS max(rate(network.total_cost))  BY tbucket = bucket(newTs, 1hour)", k8s),
            equalTo("1:49: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );

        assertThat(
            error("TS k8s | DROP @timestamp | STATS max(rate(network.total_cost))", k8s),
            equalTo("1:38: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );
    }

    public void testRenameOrDropTimestmapWithLastOverTime() {
        assertThat(
            error(
                "TS k8s | RENAME @timestamp AS newTs | STATS max(last_over_time(network.eth0.tx))  BY tbucket = bucket(newTs, 1hour)",
                k8s
            ),
            equalTo("1:49: [last_over_time(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );

        assertThat(
            error("TS k8s | DROP @timestamp | STATS max(last_over_time(network.eth0.tx))", k8s),
            equalTo("1:38: [last_over_time(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );
    }

    public void testRenameOrDropTimestmapWithFirstOverTime() {
        assertThat(
            error(
                "TS k8s | RENAME @timestamp AS newTs | STATS max(first_over_time(network.eth0.tx))  BY tbucket = bucket(newTs, 1hour)",
                k8s
            ),
            equalTo("1:49: [first_over_time(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );

        assertThat(
            error("TS k8s | DROP @timestamp | STATS max(first_over_time(network.eth0.tx))", k8s),
            equalTo("1:38: [first_over_time(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );
    }

    public void testRenameOrDropTimestmapWithIncrease() {
        assertThat(
            error("TS k8s | RENAME @timestamp AS newTs | STATS max(increase(network.eth0.tx))  BY tbucket = bucket(newTs, 1hour)", k8s),
            equalTo("1:49: [increase(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );

        assertThat(
            error("TS k8s | DROP @timestamp | STATS max(increase(network.eth0.tx))", k8s),
            equalTo("1:38: [increase(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );
    }

    public void testRenameOrDropTimestmapWithIRate() {
        assertThat(
            error("TS k8s | RENAME @timestamp AS newTs | STATS max(irate(network.eth0.tx))  BY tbucket = bucket(newTs, 1hour)", k8s),
            equalTo("1:49: [irate(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );

        assertThat(
            error("TS k8s | DROP @timestamp | STATS max(irate(network.eth0.tx))", k8s),
            equalTo("1:38: [irate(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );
    }

    public void testRenameOrDropTimestmapWithDelta() {
        assertThat(
            error("TS k8s | RENAME @timestamp AS newTs | STATS max(delta(network.eth0.tx))  BY tbucket = bucket(newTs, 1hour)", k8s),
            equalTo("1:49: [delta(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );

        assertThat(
            error("TS k8s | DROP @timestamp | STATS max(delta(network.eth0.tx))", k8s),
            equalTo("1:38: [delta(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );
    }

    public void testRenameOrDropTimestmapWithIDelta() {
        assertThat(
            error("TS k8s | RENAME @timestamp AS newTs | STATS max(idelta(network.eth0.tx))  BY tbucket = bucket(newTs, 1hour)", k8s),
            equalTo("1:49: [idelta(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );

        assertThat(
            error("TS k8s | DROP @timestamp | STATS max(idelta(network.eth0.tx))", k8s),
            equalTo("1:38: [idelta(network.eth0.tx)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );
    }

    public void testRenameOrDropTimestampWithTBucket() {
        assertThat(
            error("TS k8s | RENAME @timestamp AS newTs | STATS max(max_over_time(network.eth0.tx))  BY tbucket = tbucket(1hour)", k8s),
            equalTo("1:95: [tbucket(1hour)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );

        assertThat(
            error("TS k8s | DROP @timestamp | STATS max(max_over_time(network.eth0.tx)) BY tbucket = tbucket(1hour)", k8s),
            equalTo("1:83: [tbucket(1hour)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );
    }

    public void testAggsResolutionWithUnresolvedGroupings() {
        String agg_func = randomFrom(
            new String[] { "avg", "count", "count_distinct", "min", "max", "median", "median_absolute_deviation", "sum", "values" }
        );

        assertThat(error("FROM test | STATS " + agg_func + "(emp_no) by foobar"), matchesRegex("1:\\d+: Unknown column \\[foobar]"));
        assertThat(error("FROM test | STATS " + agg_func + "(x) by foobar, x = emp_no"), matchesRegex("1:\\d+: Unknown column \\[foobar]"));
        assertThat(error("FROM test | STATS " + agg_func + "(foobar) by foobar"), matchesRegex("1:\\d+: Unknown column \\[foobar]"));
        assertThat(
            error("FROM test | STATS " + agg_func + "(foobar) by BUCKET(hire_date, 10)"),
            matchesRegex(
                "1:\\d+: function expects exactly four arguments when the first one is of type \\[DATETIME]"
                    + " and the second of type \\[INTEGER]\n"
                    + "line 1:\\d+: Unknown column \\[foobar]"
            )
        );
        assertThat(error("FROM test | STATS " + agg_func + "(foobar) by emp_no"), matchesRegex("1:\\d+: Unknown column \\[foobar]"));
        // TODO: Ideally, we'd detect that count_distinct(x) doesn't require an error message.
        assertThat(
            error("FROM test | STATS " + agg_func + "(x) by x = foobar"),
            matchesRegex("1:\\d+: Unknown column \\[foobar]\n" + "line 1:\\d+: Unknown column \\[x]")
        );
    }

    public void testNotAllowRateOutsideMetrics() {
        assertThat(
            error("FROM test  | STATS avg(rate(network.bytes_in))", tsdb),
            equalTo("1:24: time_series aggregate[rate(network.bytes_in)] can only be used with the TS command")
        );
        assertThat(
            error("FROM test  | STATS rate(network.bytes_in)", tsdb),
            equalTo("1:20: time_series aggregate[rate(network.bytes_in)] can only be used with the TS command")
        );
        assertThat(
            error("FROM test  | STATS max_over_time(network.connections)", tsdb),
            equalTo("1:20: time_series aggregate[max_over_time(network.connections)] can only be used with the TS command")
        );
        assertThat(
            error("FROM test  | EVAL r = rate(network.bytes_in)", tsdb),
            equalTo("1:23: aggregate function [rate(network.bytes_in)] not allowed outside STATS command")
        );
    }

    public void testTimeseriesAggregate() {
        assertThat(error("TS test  | STATS max(avg(rate(network.bytes_in)))", tsdb), equalTo("""
            1:22: nested aggregations [avg(rate(network.bytes_in))] \
            not allowed inside other aggregations [max(avg(rate(network.bytes_in)))]
            line 1:12: cannot use aggregate function [avg(rate(network.bytes_in))] \
            inside over-time aggregation function [rate(network.bytes_in)]"""));

        assertThat(error("TS test  | STATS max(avg(rate(network.bytes_in)))", tsdb), equalTo("""
            1:22: nested aggregations [avg(rate(network.bytes_in))] \
            not allowed inside other aggregations [max(avg(rate(network.bytes_in)))]
            line 1:12: cannot use aggregate function [avg(rate(network.bytes_in))] \
            inside over-time aggregation function [rate(network.bytes_in)]"""));

        assertThat(
            error("TS test  | STATS COUNT(*)", tsdb),
            equalTo("1:18: count_star [COUNT(*)] can't be used with TS command; use count on a field instead")
        );
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
            "1:36: [:] operator is only supported in WHERE and STATS commands or in EVAL within score(.) function"
                + "\n"
                + "line 1:36: [:] operator cannot operate on [title], which is not a field from an index mapping",
            error("row title = \"brown fox\" | eval x = title:\"fox\" ")
        );
    }

    public void testFieldBasedFullTextFunctions() throws Exception {
        checkFieldBasedWithNonIndexedColumn("MATCH", "match(text, \"cat\")", "function");
        checkFieldBasedFunctionNotAllowedAfterCommands("MATCH", "function", "match(title, \"Meditation\")");

        checkFieldBasedWithNonIndexedColumn(":", "text : \"cat\"", "operator");
        checkFieldBasedFunctionNotAllowedAfterCommands(":", "operator", "title : \"Meditation\"");

        checkFieldBasedWithNonIndexedColumn("MatchPhrase", "match_phrase(text, \"cat\")", "function");
        checkFieldBasedFunctionNotAllowedAfterCommands("MatchPhrase", "function", "match_phrase(title, \"Meditation\")");

        if (EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.isEnabled()) {
            checkFieldBasedWithNonIndexedColumn("MultiMatch", "multi_match(\"cat\", text)", "function");
            checkFieldBasedFunctionNotAllowedAfterCommands("MultiMatch", "function", "multi_match(\"Meditation\", title)");
        }
        checkFieldBasedFunctionNotAllowedAfterCommands("KNN", "function", "knn(vector, [1, 2, 3])");
    }

    private void checkFieldBasedFunctionNotAllowedAfterCommands(String functionName, String functionType, String functionInvocation) {
        assertThat(
            error("from test | limit 10 | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] " + functionType + " cannot be used after LIMIT")
        );
        String fieldName = "KNN".equals(functionName) ? "vector" : "title";
        assertThat(
            error("from test | STATS c = COUNT(id) BY " + fieldName + " | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] " + functionType + " cannot be used after STATS")
        );
    }

    // These should pass eventually once we lift some restrictions on match function
    private void checkFieldBasedWithNonIndexedColumn(String functionName, String functionInvocation, String functionType) {
        assertThat(
            error("from test | eval text = substring(title, 1) | where " + functionInvocation, fullTextAnalyzer),
            containsString(
                "[" + functionName + "] " + functionType + " cannot operate on [text], which is not a field from an index mapping"
            )
        );
        assertThat(
            error("from test | eval text=concat(title, body) | where " + functionInvocation, fullTextAnalyzer),
            containsString(
                "[" + functionName + "] " + functionType + " cannot operate on [text], which is not a field from an index mapping"
            )
        );
        var keywordInvocation = functionInvocation.replace("text", "text::keyword");
        String keywordError = error("row n = null | eval text = n + 5 | where " + keywordInvocation, fullTextAnalyzer);
        assertThat(keywordError, containsString("[" + functionName + "] " + functionType + " cannot operate on"));
        assertThat(keywordError, containsString("which is not a field from an index mapping"));
    }

    public void testNonFieldBasedFullTextFunctionsNotAllowedAfterCommands() throws Exception {
        checkNonFieldBasedFullTextFunctionsNotAllowedAfterCommands("QSTR", "qstr(\"field_name: Meditation\")");
        checkNonFieldBasedFullTextFunctionsNotAllowedAfterCommands("KQL", "kql(\"field_name: Meditation\")");
    }

    private void checkNonFieldBasedFullTextFunctionsNotAllowedAfterCommands(String functionName, String functionInvocation) {
        // Source commands
        assertThat(
            error("show info | where " + functionInvocation),
            containsString("[" + functionName + "] function cannot be used after SHOW")
        );
        assertThat(
            error("row a= \"Meditation\" | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after ROW")
        );

        // Processing commands
        assertThat(
            error("from test | dissect title \"%{foo}\" | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after DISSECT")
        );
        assertThat(
            error("from test | drop body | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after DROP")
        );
        assertThat(
            error("from test | enrich languages on category with lang = language_name | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after ENRICH")
        );
        assertThat(
            error("from test | eval z = 2 | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after EVAL")
        );
        assertThat(
            error("from test | grok body \"%{WORD:foo}\" | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after GROK")
        );
        assertThat(
            error("from test | keep category | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after KEEP")
        );
        assertThat(
            error("from test | limit 10 | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after LIMIT")
        );
        assertThat(
            error("from test | mv_expand body | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after MV_EXPAND")
        );
        assertThat(
            error("from test | rename body as full_body | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after RENAME")
        );
        assertThat(
            error("from test | STATS c = COUNT(*) BY category | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after STATS")
        );

        // Some combination of processing commands
        assertThat(
            error("from test | keep category | limit 10 | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after LIMIT")
        );
        assertThat(
            error("from test | limit 10 | mv_expand body | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after MV_EXPAND")
        );
        assertThat(
            error("from test | mv_expand body | keep body | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after KEEP")
        );
        assertThat(
            error(
                "from test | STATS c = COUNT(id) BY category | rename c as total_categories | where " + functionInvocation,
                fullTextAnalyzer
            ),
            containsString("[" + functionName + "] function cannot be used after RENAME")
        );
        assertThat(
            error("from test | rename title as name | drop category | where " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] function cannot be used after DROP")
        );
    }

    public void testFullTextFunctionsOnlyAllowedInWhere() throws Exception {
        checkFullTextFunctionsOnlyAllowedInWhere("MATCH", "match(title, \"Meditation\")", "function");
        checkFullTextFunctionsOnlyAllowedInWhere(":", "title:\"Meditation\"", "operator");
        checkFullTextFunctionsOnlyAllowedInWhere("QSTR", "qstr(\"Meditation\")", "function");
        checkFullTextFunctionsOnlyAllowedInWhere("KQL", "kql(\"Meditation\")", "function");
        checkFullTextFunctionsOnlyAllowedInWhere("MatchPhrase", "match_phrase(title, \"Meditation\")", "function");
        checkFullTextFunctionsOnlyAllowedInWhere("KNN", "knn(vector, [0, 1, 2])", "function");
        if (EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.isEnabled()) {
            checkFullTextFunctionsOnlyAllowedInWhere("MultiMatch", "multi_match(\"Meditation\", title, body)", "function");
        }
    }

    private void checkFullTextFunctionsOnlyAllowedInWhere(String functionName, String functionInvocation, String functionType)
        throws Exception {
        assertThat(
            error("from test | eval y = " + functionInvocation, fullTextAnalyzer),
            containsString(
                "["
                    + functionName
                    + "] "
                    + functionType
                    + " is only supported in WHERE and STATS commands or in EVAL within score(.) function"
            )
        );
        assertThat(
            error("from test | sort " + functionInvocation + " asc", fullTextAnalyzer),
            containsString("[" + functionName + "] " + functionType + " is only supported in WHERE and STATS commands")

        );
        assertThat(
            error("from test | stats max_id = max(id) by " + functionInvocation, fullTextAnalyzer),
            containsString("[" + functionName + "] " + functionType + " is only supported in WHERE and STATS commands")
        );
        if ("KQL".equals(functionName) || "QSTR".equals(functionName)) {
            assertThat(
                error("row a = " + functionInvocation, fullTextAnalyzer),
                containsString(
                    "["
                        + functionName
                        + "] "
                        + functionType
                        + " is only supported in WHERE and STATS commands or in EVAL within score(.) function"
                )
            );
        }
    }

    public void testFullTextFunctionsDisjunctions() {
        checkWithFullTextFunctionsDisjunctions("match(title, \"Meditation\")");
        checkWithFullTextFunctionsDisjunctions("title : \"Meditation\"");
        checkWithFullTextFunctionsDisjunctions("qstr(\"title: Meditation\")");
        checkWithFullTextFunctionsDisjunctions("kql(\"title: Meditation\")");
        checkWithFullTextFunctionsDisjunctions("match_phrase(title, \"Meditation\")");
        checkWithFullTextFunctionsDisjunctions("knn(vector, [1, 2, 3])");
        if (EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.isEnabled()) {
            checkWithFullTextFunctionsDisjunctions("multi_match(\"Meditation\", title, body)");
        }
    }

    private void checkWithFullTextFunctionsDisjunctions(String functionInvocation) {

        // Disjunctions with non-pushable functions - scoring
        query("from test | where " + functionInvocation + " or length(title) > 10", fullTextAnalyzer);
        query("from test | where match(title, \"Meditation\") or (" + functionInvocation + " and length(title) > 10)", fullTextAnalyzer);
        query(
            "from test | where (" + functionInvocation + " and length(title) > 0) or (match(title, \"Meditation\") and length(title) > 10)",
            fullTextAnalyzer
        );

        // Disjunctions with non-pushable functions - no scoring
        query("from test | where " + functionInvocation + " or length(title) > 10", fullTextAnalyzer);
        query("from test | where match(title, \"Meditation\") or (" + functionInvocation + " and length(title) > 10)", fullTextAnalyzer);
        query(
            "from test | where (" + functionInvocation + " and length(title) > 0) or (match(title, \"Meditation\") and length(title) > 10)",
            fullTextAnalyzer
        );

        // Disjunctions with full text functions - no scoring
        query("from test | where " + functionInvocation + " or match(title, \"Meditation\")", fullTextAnalyzer);
        query("from test | where " + functionInvocation + " or not match(title, \"Meditation\")", fullTextAnalyzer);
        query("from test | where (" + functionInvocation + " or match(title, \"Meditation\")) and length(title) > 10", fullTextAnalyzer);
        query(
            "from test | where (" + functionInvocation + " or match(title, \"Meditation\")) and match(body, \"Smith\")",
            fullTextAnalyzer
        );
        query(
            "from test | where " + functionInvocation + " or (match(title, \"Meditation\") and match(body, \"Smith\"))",
            fullTextAnalyzer
        );

        // Disjunctions with full text functions - scoring
        query("from test metadata _score | where " + functionInvocation + " or match(title, \"Meditation\")", fullTextAnalyzer);
        query("from test metadata _score | where " + functionInvocation + " or not match(title, \"Meditation\")", fullTextAnalyzer);
        query(
            "from test metadata _score | where (" + functionInvocation + " or match(title, \"Meditation\")) and length(title) > 10",
            fullTextAnalyzer
        );
        query(
            "from test metadata _score | where (" + functionInvocation + " or match(title, \"Meditation\")) and match(body, \"Smith\")",
            fullTextAnalyzer
        );
        query(
            "from test metadata _score | where " + functionInvocation + " or (match(title, \"Meditation\") and match(body, \"Smith\"))",
            fullTextAnalyzer
        );
    }

    public void testFullTextFunctionsWithNonBooleanFunctions() {
        checkFullTextFunctionsWithNonBooleanFunctions("MATCH", "match(title, \"Meditation\")", "function");
        checkFullTextFunctionsWithNonBooleanFunctions(":", "title:\"Meditation\"", "operator");
        checkFullTextFunctionsWithNonBooleanFunctions("QSTR", "qstr(\"title: Meditation\")", "function");
        checkFullTextFunctionsWithNonBooleanFunctions("KQL", "kql(\"title: Meditation\")", "function");
        checkFullTextFunctionsWithNonBooleanFunctions("MatchPhrase", "match_phrase(title, \"Meditation\")", "function");
        checkFullTextFunctionsWithNonBooleanFunctions("KNN", "knn(vector, [1, 2, 3])", "function");
        if (EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.isEnabled()) {
            checkFullTextFunctionsWithNonBooleanFunctions("MultiMatch", "multi_match(\"Meditation\", title, body)", "function");
        }
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
                error("from test | where " + functionInvocation + " is not null", fullTextAnalyzer)
            );
            assertEquals(
                "1:19: Invalid condition ["
                    + functionInvocation
                    + " is null]. ["
                    + functionName
                    + "] "
                    + functionType
                    + " can't be used with ISNULL",
                error("from test | where " + functionInvocation + " is null", fullTextAnalyzer)
            );
            assertEquals(
                "1:19: Invalid condition ["
                    + functionInvocation
                    + " in (\"hello\", \"world\")]. ["
                    + functionName
                    + "] "
                    + functionType
                    + " can't be used with IN",
                error("from test | where " + functionInvocation + " in (\"hello\", \"world\")", fullTextAnalyzer)
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
            error("from test | where coalesce(" + functionInvocation + ", " + functionInvocation + ")", fullTextAnalyzer)
        );
        assertEquals(
            "1:19: argument of [concat("
                + functionInvocation
                + ", \"a\")] must be [string], found value ["
                + functionInvocation
                + "] type [boolean]",
            error("from test | where concat(" + functionInvocation + ", \"a\")", fullTextAnalyzer)
        );
    }

    public void testFullTextFunctionsTargetsExistingField() throws Exception {
        testFullTextFunctionTargetsExistingField("match(title, \"Meditation\")");
        testFullTextFunctionTargetsExistingField("title : \"Meditation\"");
        testFullTextFunctionTargetsExistingField("match_phrase(title, \"Meditation\")");
        testFullTextFunctionTargetsExistingField("knn(vector, [0, 1, 2], 10)");
        if (EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.isEnabled()) {
            testFullTextFunctionTargetsExistingField("multi_match(\"Meditation\", title)");
        }
    }

    private void testFullTextFunctionTargetsExistingField(String functionInvocation) throws Exception {
        assertThat(error("from test | keep emp_no | where " + functionInvocation), containsString("Unknown column"));
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
                "1:22: second argument of ["
                    + functionName
                    + "(network.bytes_in, 0)] must be [counter_long], found value [0] type [integer]",
                error("FROM test | eval x = " + functionName + "(network.bytes_in, 0)", tsdb)
            );

            assertEquals(
                "1:22: second argument of ["
                    + functionName
                    + "(network.bytes_in, to_long(0))] must be [counter_long], "
                    + "found value [to_long(0)] type [long]",
                error("FROM test | eval x = " + functionName + "(network.bytes_in, to_long(0))", tsdb)
            );
            assertEquals(
                "1:22: second argument of ["
                    + functionName
                    + "(network.bytes_in, 0.0)] must be [counter_long], found value [0.0] type [double]",
                error("FROM test | eval x = " + functionName + "(network.bytes_in, 0.0)", tsdb)
            );

            assertEquals(
                "1:22: third argument of ["
                    + functionName
                    + "(null, network.bytes_in, 0)] must be [counter_long], found value [0] type [integer]",
                error("FROM test | eval x = " + functionName + "(null, network.bytes_in, 0)", tsdb)
            );

            assertEquals(
                "1:22: third argument of ["
                    + functionName
                    + "(null, network.bytes_in, to_long(0))] must be [counter_long], "
                    + "found value [to_long(0)] type [long]",
                error("FROM test | eval x = " + functionName + "(null, network.bytes_in, to_long(0))", tsdb)
            );
            assertEquals(
                "1:22: third argument of ["
                    + functionName
                    + "(null, network.bytes_in, 0.0)] must be [counter_long], found value [0.0] type [double]",
                error("FROM test | eval x = " + functionName + "(null, network.bytes_in, 0.0)", tsdb)
            );
        }

        // case, a subset tests of coalesce/greatest/least
        assertEquals(
            "1:22: third argument of [case(languages == 1, salary, height)] must be [integer], found value [height] type [double]",
            error("from test | eval x = case(languages == 1, salary, height)")
        );
        assertEquals(
            "1:22: third argument of [case(name == \"a\", network.bytes_in, 0)] must be [counter_long], found value [0] type [integer]",
            error("FROM test | eval x = case(name == \"a\", network.bytes_in, 0)", tsdb)
        );
    }

    public void testConditionalFunctionsWithSupportedNonNumericTypes() {
        for (String functionName : List.of("greatest", "least")) {
            // Keyword
            query("from test | eval x = " + functionName + "(\"a\", \"b\")");
            query("from test | eval x = " + functionName + "(first_name, last_name)");
            query("from test | eval x = " + functionName + "(first_name, \"b\")");

            // Text
            // Note: In ESQL text fields are not optimized for sorting/aggregation but Greatest/Least should work if they implement
            // BytesRefEvaluator
            query("from test | eval x = " + functionName + "(title, \"b\")", fullTextAnalyzer);

            // IP
            query("from test | eval x = " + functionName + "(to_ip(\"127.0.0.1\"), to_ip(\"127.0.0.2\"))");

            // Version
            query("from test | eval x = " + functionName + "(to_version(\"1.0.0\"), to_version(\"1.1.0\"))");

            // Date
            query("from test | eval x = " + functionName + "(\"2023-01-01\" :: datetime, \"2023-01-02\" :: datetime)");
            query("from test | eval x = " + functionName + "(\"2023-01-01\" :: date_nanos, \"2023-01-02\" :: date_nanos)");
        }
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
                + "[boolean, cartesian_point, cartesian_shape, date_nanos, datetime, double, geo_point, geo_shape, "
                + "geohash, geohex, geotile, integer, ip, keyword, "
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
            error("from test  | EVAL x = birth_date + 1.5::date_period")
        );
        assertEquals(
            "1:37: argument of [to_timeduration(1)] must be [time_duration or string], found value [1] type [integer]",
            error("from test   | EVAL x = birth_date - to_timeduration(1)")
        );
        assertEquals(
            "1:45: argument of [x::date_period] must be [date_period or string], found value [x] type [double]",
            error("from test  | EVAL x = 1.5, y = birth_date + x::date_period")
        );
        assertEquals(
            "1:44: argument of [to_timeduration(x)] must be [time_duration or string], found value [x] type [integer]",
            error("from test   | EVAL x = 1, y = birth_date - to_timeduration(x)")
        );
        assertEquals(
            "1:64: argument of [x::date_period] must be [date_period or string], found value [x] type [datetime]",
            error("from test  | EVAL x = \"2024-09-08\"::datetime, y = birth_date + x::date_period")
        );
        assertEquals(
            "1:65: argument of [to_timeduration(x)] must be [time_duration or string], found value [x] type [datetime]",
            error("from test   | EVAL x = \"2024-09-08\"::datetime, y = birth_date - to_timeduration(x)")
        );
        assertEquals(
            "1:58: argument of [x::date_period] must be [date_period or string], found value [x] type [ip]",
            error("from test  | EVAL x = \"2024-09-08\"::ip, y = birth_date + x::date_period")
        );
        assertEquals(
            "1:59: argument of [to_timeduration(x)] must be [time_duration or string], found value [x] type [ip]",
            error("from test   | EVAL x = \"2024-09-08\"::ip, y = birth_date - to_timeduration(x)")
        );
    }

    public void testIntervalAsString() {
        // DateTrunc
        for (String interval : List.of("1 minu", "1 dy", "1.5 minutes", "0.5 days", "minutes 1", "day 5")) {
            assertThat(
                error("from test   | EVAL x = date_trunc(\"" + interval + "\", \"1991-06-26T00:00:00.000Z\")"),
                containsString("1:35: Cannot convert string [" + interval + "] to [DATE_PERIOD or TIME_DURATION]")
            );
            assertThat(
                error("from test   | EVAL x = \"1991-06-26T00:00:00.000Z\", y = date_trunc(\"" + interval + "\", x::datetime)"),
                containsString("1:67: Cannot convert string [" + interval + "] to [DATE_PERIOD or TIME_DURATION]")
            );
        }
        for (String interval : List.of("1", "0.5", "invalid")) {
            assertThat(
                error("from test   | EVAL x = date_trunc(\"" + interval + "\", \"1991-06-26T00:00:00.000Z\")"),
                containsString(
                    "1:24: first argument of [date_trunc(\""
                        + interval
                        + "\", \"1991-06-26T00:00:00.000Z\")] must be [dateperiod or timeduration], found value [\""
                        + interval
                        + "\"] type [keyword]"
                )
            );
            assertThat(
                error("from test   | EVAL x = \"1991-06-26T00:00:00.000Z\", y = date_trunc(\"" + interval + "\", x::datetime)"),
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

    public void testCategorizeInvalidOptionsField() {
        assumeTrue("categorize options must be enabled", EsqlCapabilities.Cap.CATEGORIZE_OPTIONS.isEnabled());

        assertEquals(
            "1:31: second argument of [CATEGORIZE(last_name, first_name)] must be a map expression, received [first_name]",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, first_name)")
        );
        assertEquals(
            "1:31: Invalid option [blah] in [CATEGORIZE(last_name, { \"blah\": 42 })], "
                + "expected one of [analyzer, output_format, similarity_threshold]",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"blah\": 42 })")
        );
    }

    public void testCategorizeOptionOutputFormat() {
        assumeTrue("categorize options must be enabled", EsqlCapabilities.Cap.CATEGORIZE_OPTIONS.isEnabled());

        query("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"output_format\": \"regex\" })");
        query("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"output_format\": \"REGEX\" })");
        query("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"output_format\": \"tokens\" })");
        query("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"output_format\": \"ToKeNs\" })");
        assertEquals(
            "1:31: invalid output format [blah], expecting one of [REGEX, TOKENS]",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"output_format\": \"blah\" })")
        );
        assertEquals(
            "1:31: invalid output format [42], expecting one of [REGEX, TOKENS]",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"output_format\": 42 })")
        );
        assertEquals(
            "1:31: Invalid option [output_format] in [CATEGORIZE(last_name, { \"output_format\": { \"a\": 123 } })],"
                + " expected a [KEYWORD] value",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"output_format\": { \"a\": 123 } })")
        );
    }

    public void testCategorizeOptionSimilarityThreshold() {
        assumeTrue("categorize options must be enabled", EsqlCapabilities.Cap.CATEGORIZE_OPTIONS.isEnabled());

        query("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"similarity_threshold\": 1 })");
        query("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"similarity_threshold\": 100 })");
        assertEquals(
            "1:31: invalid similarity threshold [0], expecting a number between 1 and 100, inclusive",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"similarity_threshold\": 0 })")
        );
        assertEquals(
            "1:31: invalid similarity threshold [101], expecting a number between 1 and 100, inclusive",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"similarity_threshold\": 101 })")
        );
        assertEquals(
            "1:31: Invalid option [similarity_threshold] in [CATEGORIZE(last_name, { \"similarity_threshold\": \"blah\" })], "
                + "cannot cast [blah] to [integer]",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"similarity_threshold\": \"blah\" })")
        );
        assertEquals(
            "1:31: Invalid option [similarity_threshold] in [CATEGORIZE(last_name, { \"similarity_threshold\": { \"aaa\": 123 } })],"
                + " expected a [INTEGER] value",
            error("FROM test | STATS COUNT(*) BY CATEGORIZE(last_name, { \"similarity_threshold\": { \"aaa\": 123 } })")
        );
    }

    public void testCategorizeWithInlineStats() {
        assumeTrue("CATEGORIZE must be enabled", EsqlCapabilities.Cap.CATEGORIZE_V6.isEnabled());
        assumeTrue("INLINE STATS must be enabled", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertEquals(
            "1:38: CATEGORIZE [CATEGORIZE(last_name, { \"similarity_threshold\": 1 })] is not yet supported with "
                + "INLINE STATS [INLINE STATS COUNT(*) BY CATEGORIZE(last_name, { \"similarity_threshold\": 1 })]",
            error("FROM test | INLINE STATS COUNT(*) BY CATEGORIZE(last_name, { \"similarity_threshold\": 1 })")
        );

        assertEquals("""
            3:36: CATEGORIZE [CATEGORIZE(gender)] is not yet supported with \
            INLINE STATS [INLINE STATS SUM(salary) BY c3 = CATEGORIZE(gender)]
            line 2:92: CATEGORIZE grouping function [CATEGORIZE(first_name)] can only be in the first grouping expression
            line 2:33: CATEGORIZE [CATEGORIZE(last_name, { "similarity_threshold": 1 })] is not yet supported with \
            INLINE STATS [INLINE STATS COUNT(*) BY c1 = CATEGORIZE(last_name, { "similarity_threshold": 1 }), \
            c2 = CATEGORIZE(first_name)]""", error("""
            FROM test
            | INLINE STATS COUNT(*) BY c1 = CATEGORIZE(last_name, { "similarity_threshold": 1 }), c2 = CATEGORIZE(first_name)
            | INLINE STATS SUM(salary) BY c3 = CATEGORIZE(gender)
            """));
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
        List<DataType> unsortableTypes = EsqlCapabilities.Cap.SPATIAL_GRID_TYPES.isEnabled()
            ? List.of(CARTESIAN_POINT, CARTESIAN_SHAPE, GEO_POINT, GEO_SHAPE, GEOHASH, GEOTILE, GEOHEX)
            : List.of(CARTESIAN_POINT, CARTESIAN_SHAPE, GEO_POINT, GEO_SHAPE);
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
        List<DataType> nonNumericTypes = EsqlCapabilities.Cap.SPATIAL_GRID_TYPES.isEnabled()
            ? List.of(
                BOOLEAN,
                CARTESIAN_POINT,
                CARTESIAN_SHAPE,
                DATE_NANOS,
                DATETIME,
                GEO_POINT,
                GEO_SHAPE,
                GEOHASH,
                GEOTILE,
                GEOHEX,
                IP,
                KEYWORD,
                VERSION
            )
            : List.of(BOOLEAN, CARTESIAN_POINT, CARTESIAN_SHAPE, DATE_NANOS, DATETIME, GEO_POINT, GEO_SHAPE, IP, KEYWORD, VERSION);
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
        assertEquals(
            "1:18: aggregate function [present(gender)] not allowed outside STATS command",
            error("FROM test | SORT present(gender)")
        );
    }

    public void testFilterByAggregate() {
        assertEquals("1:19: aggregate function [count(*)] not allowed outside STATS command", error("ROW a = 1 | WHERE count(*) > 0"));
        assertEquals(
            "1:29: aggregate function [count(*)] not allowed outside STATS command",
            error("ROW a = 1 | WHERE to_string(count(*)) IS NOT NULL")
        );
        assertEquals("1:23: aggregate function [max(a)] not allowed outside STATS command", error("ROW a = 1 | WHERE 1 + max(a) > 0"));
        assertEquals(
            "1:19: aggregate function [min(languages)] not allowed outside STATS command",
            error("FROM test | WHERE min(languages) > 2")
        );
        assertEquals(
            "1:19: aggregate function [present(gender)] not allowed outside STATS command",
            error("FROM test | WHERE present(gender)")
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
        assertEquals("1:9: aggregate function [present(123)] not allowed outside STATS command", error("ROW a = present(123)"));
    }

    public void testLookupJoinDataTypeMismatch() {
        query("FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code");

        assertEquals(
            "1:87: JOIN left field [language_code] of type [KEYWORD] is incompatible with right field [language_code] of type [INTEGER]",
            error("FROM test | EVAL language_code = languages::keyword | LOOKUP JOIN languages_lookup ON language_code")
        );
    }

    public void testLookupJoinExpressionAmbiguousRight() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        String queryString = """
            from test
            | rename languages as language_code
            | lookup join languages_lookup ON salary == language_code
            """;

        assertEquals(
            " ambiguous reference to [language_code]; matches any of [line 2:10 [language_code], line 3:15 [language_code]]",
            error(queryString)
        );
    }

    public void testLookupJoinExpressionRightNotPushable() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | rename languages as languages_left
            | lookup join languages_lookup ON languages_left == language_code and abs(salary) > 1000
            """;

        assertEquals(
            "3:71: Unsupported join filter expression:abs(salary) > 1000",
            error(queryString, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION)
        );
    }

    public void testLookupJoinExpressionConstant() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | rename languages as languages_left
            | lookup join languages_lookup ON false and languages_left == language_code
            """;

        assertEquals("3:35: Unsupported join filter expression:false", error(queryString, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION));
    }

    public void testLookupJoinExpressionTranslatableButFromLeft() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | rename languages as languages_left
            | lookup join languages_lookup ON languages_left == language_code and languages_left == "English"
            """;

        assertEquals(
            "3:71: Unsupported join filter expression:languages_left == \"English\"",
            error(queryString, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION)
        );
    }

    public void testLookupJoinExpressionTranslatableButMixedLeftRight() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | rename languages as languages_left
            | lookup join languages_lookup ON languages_left == language_code and CONCAT(languages_left, language_code) == "English"
            """;

        assertEquals(
            "3:71: Unsupported join filter expression:CONCAT(languages_left, language_code) == \"English\"",
            error(queryString, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION)
        );
    }

    public void testLookupJoinExpressionComplexFormula() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | rename languages as languages_left
            | lookup join languages_lookup ON languages_left == language_code AND STARTSWITH(languages_left, language_code)
            """;

        assertEquals(
            "3:71: Unsupported join filter expression:STARTSWITH(languages_left, language_code)",
            error(queryString, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION)
        );
    }

    public void testLookupJoinExpressionAmbiguousLeft() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        String queryString = """
             from test
            | rename languages as language_name
            | lookup join languages_lookup ON language_name == language_code
            """;

        assertEquals(
            " ambiguous reference to [language_name]; matches any of [line 2:10 [language_name], line 3:15 [language_name]]",
            error(queryString)
        );
    }

    public void testLookupJoinExpressionAmbiguousBoth() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        String queryString = """
            from test
            | rename languages as language_code
            | lookup join languages_lookup ON language_code != language_code
            """;

        assertEquals(
            " ambiguous reference to [language_code]; matches any of [line 2:10 [language_code], line 3:15 [language_code]]",
            error(queryString)
        );
    }

    public void testFullTextFunctionOptions() {
        checkOptionDataTypes(Match.ALLOWED_OPTIONS, "FROM test | WHERE match(title, \"Jean\", {\"%s\": %s})");
        checkOptionDataTypes(QueryString.ALLOWED_OPTIONS, "FROM test | WHERE QSTR(\"title: Jean\", {\"%s\": %s})");
        checkOptionDataTypes(MatchPhrase.ALLOWED_OPTIONS, "FROM test | WHERE MATCH_PHRASE(title, \"Jean\", {\"%s\": %s})");
        checkOptionDataTypes(Knn.ALLOWED_OPTIONS, "FROM test | WHERE KNN(vector, [0.1, 0.2, 0.3], {\"%s\": %s})");
        if (EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.isEnabled()) {
            checkOptionDataTypes(MultiMatch.OPTIONS, "FROM test | WHERE MULTI_MATCH(\"Jean\", title, body, {\"%s\": %s})");
        }
        if (EsqlCapabilities.Cap.KQL_FUNCTION_OPTIONS.isEnabled()) {
            checkOptionDataTypes(Kql.ALLOWED_OPTIONS, "FROM test | WHERE KQL(\"title: Jean\", {\"%s\": %s})");
        }
    }

    /**
     * Check all data types for available options. When conversion is not possible, checks that it's an error
     */
    private void checkOptionDataTypes(Map<String, DataType> allowedOptionsMap, String queryTemplate) {
        DataType[] optionTypes = new DataType[] { INTEGER, LONG, FLOAT, DOUBLE, KEYWORD, BOOLEAN };
        for (Map.Entry<String, DataType> allowedOptions : allowedOptionsMap.entrySet()) {
            String optionName = allowedOptions.getKey();
            DataType optionType = allowedOptions.getValue();

            // Check every possible type for the option - we'll try to convert it to the expected type
            for (DataType currentType : optionTypes) {
                String optionValue = exampleValueForType(currentType);
                String queryOptionValue = optionValue;
                if (currentType == KEYWORD) {
                    queryOptionValue = "\"" + optionValue + "\"";
                }

                String query = String.format(Locale.ROOT, queryTemplate, optionName, queryOptionValue);
                try {
                    // Check conversion is possible
                    DataTypeConverter.convert(optionValue, optionType);
                    // If no exception was thrown, conversion is possible and should be done
                    query(query, fullTextAnalyzer);
                } catch (InvalidArgumentException e) {
                    // Conversion is not possible, query should fail
                    String error = error(query, fullTextAnalyzer);
                    assertThat(error, containsString("Invalid option [" + optionName + "]"));
                    assertThat(error, containsString("cannot cast [" + optionValue + "] to [" + optionType.typeName() + "]"));
                }
            }

            // MapExpression are not allowed as option values
            String query = String.format(Locale.ROOT, queryTemplate, optionName, "{ \"abc\": 123 }");

            assertThat(error(query, fullTextAnalyzer), containsString("Invalid option [" + optionName + "]"));
        }

        String errorQuery = String.format(Locale.ROOT, queryTemplate, "unknown_option", "\"any_value\"");
        assertThat(error(errorQuery, fullTextAnalyzer), containsString("Invalid option [unknown_option]"));
    }

    private static String exampleValueForType(DataType currentType) {
        return switch (currentType) {
            case BOOLEAN -> String.valueOf(randomBoolean());
            case INTEGER -> String.valueOf(randomIntBetween(0, 100000));
            case LONG -> String.valueOf(randomLong());
            case FLOAT -> String.valueOf(randomFloat());
            case DOUBLE -> String.valueOf(randomDouble());
            case KEYWORD -> randomAlphaOfLength(10);
            default -> throw new IllegalArgumentException("Unsupported option type: " + currentType);
        };
    }

    // Should pass eventually once we lift some restrictions on full text search functions.
    public void testFullTextFunctionCurrentlyUnsupportedBehaviour() throws Exception {
        testFullTextFunctionsCurrentlyUnsupportedBehaviour("match(title, \"Meditation\")");
        testFullTextFunctionsCurrentlyUnsupportedBehaviour("title : \"Meditation\"");
        testFullTextFunctionsCurrentlyUnsupportedBehaviour("match_phrase(title, \"Meditation\")");
        if (EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.isEnabled()) {
            testFullTextFunctionsCurrentlyUnsupportedBehaviour("multi_match(\"Meditation\", title)");
        }
    }

    private void testFullTextFunctionsCurrentlyUnsupportedBehaviour(String functionInvocation) throws Exception {
        assertThat(
            error("from test | stats max_salary = max(salary) by emp_no | where " + functionInvocation, fullTextAnalyzer),
            containsString("Unknown column")
        );
    }

    public void testFullTextFunctionsNullArgs() throws Exception {
        checkFullTextFunctionNullArgs("match(title, null)", "second");
        checkFullTextFunctionAcceptsNullField("match(null, \"test\")");
        checkFullTextFunctionNullArgs("qstr(null)", "");
        checkFullTextFunctionNullArgs("kql(null)", "");
        checkFullTextFunctionNullArgs("match_phrase(title, null)", "second");
        checkFullTextFunctionAcceptsNullField("match_phrase(null, \"test\")");
        checkFullTextFunctionNullArgs("knn(vector, null)", "second");
        checkFullTextFunctionAcceptsNullField("knn(null, [0, 1, 2])");
        if (EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.isEnabled()) {
            checkFullTextFunctionNullArgs("multi_match(null, title)", "first");
            checkFullTextFunctionAcceptsNullField("multi_match(\"test\", null)");
        }
    }

    private void checkFullTextFunctionNullArgs(String functionInvocation, String argOrdinal) throws Exception {
        assertThat(
            error("from test | where " + functionInvocation, fullTextAnalyzer),
            containsString(argOrdinal + " argument of [" + functionInvocation + "] cannot be null, received [null]")
        );
    }

    private void checkFullTextFunctionAcceptsNullField(String functionInvocation) throws Exception {
        fullTextAnalyzer.analyze(EsqlParser.INSTANCE.parseQuery("from test | where " + functionInvocation));
    }

    public void testInsistNotOnTopOfFrom() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        assertThat(
            error("FROM test | EVAL foo = 42 | INSIST_🐔 bar"),
            containsString("1:29: [insist] can only be used after [from] or [insist] commands, but was [EVAL foo = 42]")
        );
    }

    public void testFullTextFunctionsInStats() {
        checkFullTextFunctionsInStats("match(title, \"Meditation\")");
        checkFullTextFunctionsInStats("title : \"Meditation\"");
        checkFullTextFunctionsInStats("qstr(\"title: Meditation\")");
        checkFullTextFunctionsInStats("kql(\"title: Meditation\")");
        checkFullTextFunctionsInStats("match_phrase(title, \"Meditation\")");
        checkFullTextFunctionsInStats("knn(vector, [0, 1, 2])");
        if (EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.isEnabled()) {
            checkFullTextFunctionsInStats("multi_match(\"Meditation\", title, body)");
        }
    }

    public void testDecayArgs() {
        // First arg cannot be null
        assertEquals(
            "2:23: first argument of [decay(null, origin, scale, "
                + "{\"offset\": 0, \"decay\": 0.5, \"type\": \"linear\"})] cannot be null, received [null]",
            error(
                "row origin = 10, scale = 10\n"
                    + "| eval decay_result = decay(null, origin, scale, {\"offset\": 0, \"decay\": 0.5, \"type\": \"linear\"})"
            )
        );

        // Second arg cannot be null
        assertEquals(
            "2:23: second argument of [decay(value, null, scale, "
                + "{\"offset\": 0, \"decay\": 0.5, \"type\": \"linear\"})] cannot be null, received [null]",
            error(
                "row value = 10, scale = 10\n"
                    + "| eval decay_result = decay(value, null, scale, {\"offset\": 0, \"decay\": 0.5, \"type\": \"linear\"})"
            )
        );

        // Third arg cannot be null
        assertEquals(
            "2:23: third argument of [decay(value, origin, null, "
                + "{\"offset\": 0, \"decay\": 0.5, \"type\": \"linear\"})] cannot be null, received [null]",
            error(
                "row value = 10, origin = 10\n"
                    + "| eval decay_result = decay(value, origin, null, {\"offset\": 0, \"decay\": 0.5, \"type\": \"linear\"})"
            )
        );

        // Offset value type
        assertEquals(
            "2:23: offset option has invalid type, expected [numeric], found [keyword]",
            error(
                "row value = 10, origin = 10, scale = 1\n"
                    + "| eval decay_result = decay(value, origin, scale, {\"offset\": \"aaa\", \"decay\": 0.5, \"type\": \"linear\"})"
            )
        );

        assertEquals(
            "1:118: offset option has invalid type, expected [time_duration], found [keyword]",
            error(
                "row value =  TO_DATETIME(\"2023-01-01T00:00:00Z\"), origin =  TO_DATETIME(\"2023-01-01T00:00:00Z\")"
                    + "| eval decay_result = decay(value, origin, 24 hours, {\"offset\": \"aaa\", \"decay\": 0.5, \"type\": \"linear\"})"
            )
        );

        assertEquals(
            "1:110: offset option has invalid type, expected [numeric], found [keyword]",
            error(
                "row value =  TO_CARTESIANPOINT(\"POINT(10 0)\"), origin = TO_CARTESIANPOINT(\"POINT(0 0)\")"
                    + "| eval decay_result = decay(value, origin, 10.0, {\"offset\": \"aaa\", \"decay\": 0.5, \"type\": \"linear\"})"
            )
        );

        // Type option value
        assertEquals(
            "2:23: Invalid option [type] in "
                + "[decay(value, origin, scale, {\"offset\": 1, \"decay\": 0.5, \"type\": 123})], allowed types [[KEYWORD]]",
            error(
                "row value = 10, origin = 10, scale = 1\n"
                    + "| eval decay_result = decay(value, origin, scale, {\"offset\": 1, \"decay\": 0.5, \"type\": 123})"
            )
        );

        assertEquals(
            "2:23: type option has invalid value, expected one of [gauss, linear, exp], found [\"foobar\"]",
            error(
                "row value = 10, origin = 10, scale = 1\n"
                    + "| eval decay_result = decay(value, origin, scale, {\"offset\": 1, \"decay\": 0.5, \"type\": \"foobar\"})"
            )
        );
    }

    private void checkFullTextFunctionsInStats(String functionInvocation) {
        query("from test | stats c = max(id) where " + functionInvocation, fullTextAnalyzer);
        query("from test | stats c = max(id) where " + functionInvocation + " or length(title) > 10", fullTextAnalyzer);
        query("from test metadata _score |  where " + functionInvocation + " | stats c = max(_score)", fullTextAnalyzer);
        query(
            "from test metadata _score |  where " + functionInvocation + " or length(title) > 10 | stats c = max(_score)",
            fullTextAnalyzer
        );

        assertThat(
            error("from test metadata _score | stats c = max(_score) where " + functionInvocation, fullTextAnalyzer),
            containsString("cannot use _score aggregations with a WHERE filter in a STATS command")
        );
    }

    public void testVectorSimilarityFunctionsNullArgs() throws Exception {
        checkVectorFunctionsNullArgs("v_cosine(null, vector)");
        checkVectorFunctionsNullArgs("v_cosine(vector, null)");
        checkVectorFunctionsNullArgs("v_dot_product(null, vector)");
        checkVectorFunctionsNullArgs("v_dot_product(vector, null)");
        checkVectorFunctionsNullArgs("v_l1_norm(null, vector)");
        checkVectorFunctionsNullArgs("v_l1_norm(vector, null)");
        checkVectorFunctionsNullArgs("v_l2_norm(null, vector)");
        checkVectorFunctionsNullArgs("v_l2_norm(vector, null)");
        checkVectorFunctionsNullArgs("v_hamming(null, vector)");
        checkVectorFunctionsNullArgs("v_hamming(vector, null)");
        if (EsqlCapabilities.Cap.MAGNITUDE_SCALAR_VECTOR_FUNCTION.isEnabled()) {
            checkVectorFunctionsNullArgs("v_magnitude(null)");
        }
    }

    public void testFullTextFunctionsWithSemanticText() {
        checkFullTextFunctionsWithSemanticText("knn(semantic, [0, 1, 2])");
        checkFullTextFunctionsWithSemanticText("match(semantic, \"hello world\")");
        checkFullTextFunctionsWithSemanticText("semantic:\"hello world\"");
    }

    private void checkFullTextFunctionsWithSemanticText(String functionInvocation) {
        query("from test | where " + functionInvocation, fullTextAnalyzer);
    }

    public void testToIPInvalidOptions() {
        String query = "ROW result = to_ip(\"127.0.0.1\", 123)";
        assertThat(error(query), containsString("second argument of [to_ip(\"127.0.0.1\", 123)] must be a map expression, received [123]"));

        query = "ROW result = to_ip(\"127.0.0.1\", { \"leading_zeros\": { \"foo\": \"bar\" } })";
        assertThat(error(query), containsString("Invalid option [leading_zeros]"));

        query = "ROW result = to_ip(\"127.0.0.1\", { \"leading_zeros\": \"abcdef\" })";
        assertThat(error(query), containsString("Illegal leading_zeros [abcdef]"));
    }

    public void testInvalidTBucketCalls() {
        assertThat(
            error("from test | stats max(emp_no) by tbucket(1 hour)"),
            equalTo("1:34: [tbucket(1 hour)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );
        assertThat(
            error("from test | stats max(event_duration) by tbucket()", sampleDataAnalyzer, ParsingException.class),
            equalTo("1:42: error building [tbucket]: expects exactly one argument")
        );
        assertThat(
            error("from test | stats max(event_duration) by tbucket(\"@tbucket\", 1 hour)", sampleDataAnalyzer, ParsingException.class),
            equalTo("1:42: error building [tbucket]: expects exactly one argument")
        );
        assertThat(
            error("from test | stats max(event_duration) by tbucket(1 hr)", sampleDataAnalyzer, ParsingException.class),
            equalTo("1:50: Unexpected temporal unit: 'hr'")
        );
        assertThat(
            error("from test | stats max(event_duration) by tbucket(\"1\")", sampleDataAnalyzer),
            equalTo("1:42: argument of [tbucket(\"1\")] must be [date_period or time_duration], found value [\"1\"] type [keyword]")
        );

        /*
        To test unsupported @timestamp data type. In this case, we use a boolean as a type for the @timestamp field which is not supported
        by TBUCKET.
         */
        assertThat(
            error("from test | stats max(event_duration) by tbucket(\"1 hour\")", oddSampleDataAnalyzer),
            equalTo(
                "1:42: implicit argument of [tbucket(\"1 hour\")] must be [date_nanos or datetime], found value [@timestamp] type [boolean]"
            )
        );
        for (String interval : List.of("1 minu", "1 dy", "1.5 minutes", "0.5 days", "minutes 1", "day 5")) {
            assertThat(
                error("from test | stats max(event_duration) by tbucket(\"" + interval + "\")", sampleDataAnalyzer),
                containsString("1:50: Cannot convert string [" + interval + "] to [DATE_PERIOD or TIME_DURATION]")
            );
        }
    }

    public void testFuse() {
        String queryPrefix = "from test metadata _score, _index, _id | fork (where true) (where true)";

        query(queryPrefix + " | fuse");
        query(queryPrefix + " | fuse rrf");
        query(queryPrefix + " | fuse rrf with { \"rank_constant\": 123 } ");
        query(queryPrefix + " | fuse rrf with { \"weights\": { \"fork1\":  123 } }");
        query(queryPrefix + " | fuse rrf with { \"rank_constant\": 123, \"weights\": { \"fork1\":  123 } }");

        query(queryPrefix + " | fuse with { \"rank_constant\": 123 } ");
        query(queryPrefix + " | fuse with { \"weights\": { \"fork1\":  123 } }");
        query(queryPrefix + " | fuse with { \"rank_constant\": 123, \"weights\": { \"fork1\":  123 } }");

        query(queryPrefix + " | fuse linear");
        query(queryPrefix + " | fuse linear with { \"normalizer\": \"minmax\" } ");
        query(queryPrefix + " | fuse linear with { \"weights\": { \"fork1\":  123 } }");

        query(queryPrefix + " | fuse linear score by _score with { \"normalizer\": \"minmax\" } ");
        query(queryPrefix + " | eval new_score = _score + 1 | fuse linear score by new_score with { \"normalizer\": \"minmax\" } ");
        query(queryPrefix + " | fuse linear group by _fork with { \"normalizer\": \"minmax\" } ");
        query(queryPrefix + " | eval new_fork = to_upper(_fork) | fuse linear group by new_fork with { \"normalizer\": \"minmax\" } ");
        query(queryPrefix + " | fuse linear key by _id,_index with { \"normalizer\": \"minmax\" } ");
        query(queryPrefix + " | eval new_id = concat(_id, _index) | fuse linear key by new_id with { \"normalizer\": \"minmax\" } ");
        query(queryPrefix + " | fuse linear score by _score key by _id, _index group by _fork with { \"normalizer\": \"minmax\" } ");

        assertThat(error(queryPrefix + " | fuse rrf WITH { \"abc\": 123 }"), containsString("unknown option [abc]"));

        assertThat(
            error(queryPrefix + " | fuse rrf WITH { \"rank_constant\": \"a\" }"),
            containsString("expected rank_constant to be numeric, got [\"a\"]")
        );

        assertThat(
            error(queryPrefix + " | fuse rrf WITH { \"rank_constant\": { \"a\": 123 } }"),
            containsString("expected rank_constant to be a literal")
        );

        assertThat(
            error(queryPrefix + " | fuse rrf WITH { \"rank_constant\": -123 }"),
            containsString("expected rank_constant to be positive, got [-123]")
        );

        assertThat(
            error(queryPrefix + " | fuse rrf WITH { \"rank_constant\": 0 }"),
            containsString("expected rank_constant to be positive, got [0]")
        );

        for (var fuseMethod : List.of("rrf", "linear")) {
            assertThat(
                error(queryPrefix + " | fuse " + fuseMethod + " WITH { \"weights\": 123 }"),
                containsString("expected weights to be a MapExpression")
            );

            assertThat(
                error(queryPrefix + " | fuse " + fuseMethod + " WITH { \"weights\": { \"fork1\": \"a\" } }"),
                containsString("expected weight to be numeric")
            );

            assertThat(
                error(queryPrefix + " | fuse " + fuseMethod + " WITH { \"weights\": { \"fork1\": { \"a\": 123 } } }"),
                containsString("expected weight to be a literal")
            );

            assertThat(
                error(queryPrefix + " | fuse " + fuseMethod + " WITH { \"weights\": { \"fork1\": -123 } }"),
                containsString("expected weight to be positive, got [-123]")
            );

            assertThat(
                error(queryPrefix + " | fuse " + fuseMethod + " WITH { \"weights\": { \"fork1\": 1, \"fork2\": 0 } }"),
                containsString("expected weight to be positive, got [0]")
            );
        }

        assertThat(error(queryPrefix + " | fuse linear WITH { \"abc\": 123 }"), containsString("unknown option [abc]"));

        assertThat(
            error(queryPrefix + " | fuse linear WITH { \"normalizer\": 123 }"),
            containsString("expected normalizer to be a string, got [123]")
        );

        assertThat(
            error(queryPrefix + " | fuse linear WITH { \"normalizer\": { \"a\": 123 } }"),
            containsString("expected normalizer to be a literal")
        );

        assertThat(
            error(queryPrefix + " | fuse linear WITH { \"normalizer\": \"foo\" }"),
            containsString("[\"foo\"] is not a valid normalizer")
        );

        assertThat(error(queryPrefix + " | fuse linear SCORE BY foobar"), containsString("Unknown column [foobar]"));

        assertThat(error(queryPrefix + " | fuse linear GROUP BY foobar"), containsString("Unknown column [foobar]"));

        assertThat(error(queryPrefix + " | fuse linear KEY BY _id, foobar"), containsString("Unknown column [foobar]"));

        assertThat(
            error(queryPrefix + " | fuse linear SCORE BY first_name"),
            containsString("expected SCORE BY column [first_name] to be DOUBLE, not KEYWORD")
        );

        assertThat(
            error(queryPrefix + " | fuse linear GROUP BY _score"),
            containsString("expected GROUP BY field [_score] to be KEYWORD or TEXT, not DOUBLE")
        );

        assertThat(
            error(queryPrefix + " | fuse linear KEY BY _score"),
            containsString("expected KEY BY field [_score] to be KEYWORD or TEXT, not DOUBLE")
        );

        assertThat(
            error("FROM test METADATA _index, _score, _id | EVAL _fork = \"fork1\" | FUSE"),
            containsString("FUSE can only be used on a limited number of rows. Consider adding a LIMIT before FUSE.")
        );

        assertThat(
            error("FROM test | LIMIT 10 | FUSE"),
            equalTo(
                "1:24: FUSE requires a score column, default [_score] column not found.\n"
                    + "line 1:24: FUSE requires a column to group by, default [_fork] column not found.\n"
                    + "line 1:24: FUSE requires a key column, default [_id] column not found\n"
                    + "line 1:24: FUSE requires a key column, default [_index] column not found"
            )
        );

        if (EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled()) {
            assertThat(error("""
                FROM (FROM test METADATA _index, _id, _score | EVAL label = "query1"),
                     (FROM test METADATA _index, _id, _score | EVAL label = "query2" | LIMIT 10)
                | FUSE GROUP BY label
                """), containsString("FUSE can only be used on a limited number of rows. Consider adding a LIMIT before FUSE."));
        }
    }

    public void testNoMetricInStatsByClause() {
        assertThat(
            error("TS test | STATS avg(rate(network.bytes_in)) BY bucket(@timestamp, 1 minute), host, round(network.connections)", tsdb),
            equalTo(
                "1:90: cannot group by a metric field [network.connections] in a time-series aggregation. "
                    + "If you want to group by a metric field, use the FROM command instead of the TS command."
            )
        );
        assertThat(
            error("TS test | STATS avg(rate(network.bytes_in)) BY bucket(@timestamp, 1 minute), host, network.bytes_in", tsdb),
            equalTo("1:84: cannot group by on [counter_long] type for grouping [network.bytes_in]")
        );
        assertThat(
            error("TS test | STATS avg(rate(network.bytes_in)) BY bucket(@timestamp, 1 minute), host, to_long(network.bytes_in)", tsdb),
            equalTo(
                "1:92: cannot group by a metric field [network.bytes_in] in a time-series aggregation. "
                    + "If you want to group by a metric field, use the FROM command instead of the TS command."
            )
        );
    }

    public void testNoDimensionsInAggsOnlyInByClause() {
        assertThat(
            error("TS test | STATS count(host) BY bucket(@timestamp, 1 minute)", tsdb),
            equalTo(
                "1:23: argument of [implicit time-series aggregation function (LastOverTime) for host] must be "
                    + "[numeric except unsigned_long], found value [host] type [keyword]; "
                    + "to aggregate non-numeric fields, use the FROM command instead of the TS command"
            )
        );
        assertThat(
            error("TS test | STATS max(name) BY bucket(@timestamp, 1 minute)", tsdb),
            equalTo(
                "1:21: argument of [implicit time-series aggregation function (LastOverTime) for name] must be "
                    + "[numeric except unsigned_long], found value [name] type [keyword]; "
                    + "to aggregate non-numeric fields, use the FROM command instead of the TS command"
            )
        );
    }

    public void testSortInTimeSeries() {
        assertThat(
            error("TS test | SORT host | STATS avg(last_over_time(network.connections))", tsdb),
            equalTo(
                "1:11: sorting [SORT host] between the time-series source "
                    + "and the first aggregation [STATS avg(last_over_time(network.connections))] is not allowed"
            )
        );
        assertThat(
            error("TS test | LIMIT 10 | STATS avg(network.connections)", tsdb),
            equalTo(
                "1:11: limiting [LIMIT 10] the time-series source before the first aggregation "
                    + "[STATS avg(network.connections)] is not allowed; filter data with a WHERE command instead"
            )
        );
        assertThat(error("TS test | SORT host | LIMIT 10 | STATS avg(network.connections)", tsdb), equalTo("""
            1:23: limiting [LIMIT 10] the time-series source \
            before the first aggregation [STATS avg(network.connections)] is not allowed; filter data with a WHERE command instead
            line 1:11: sorting [SORT host] between the time-series source \
            and the first aggregation [STATS avg(network.connections)] is not allowed"""));
    }

    public void testTextEmbeddingFunctionInvalidQuery() {
        assertThat(
            error("from test | EVAL embedding = TEXT_EMBEDDING(null, ?)", defaultAnalyzer, TEXT_EMBEDDING_INFERENCE_ID),
            equalTo("1:30: first argument of [TEXT_EMBEDDING(null, ?)] cannot be null, received [null]")
        );

        assertThat(
            error("from test | EVAL embedding = TEXT_EMBEDDING(42, ?)", defaultAnalyzer, TEXT_EMBEDDING_INFERENCE_ID),
            equalTo("1:30: first argument of [TEXT_EMBEDDING(42, ?)] must be [string], found value [42] type [integer]")
        );

        assertThat(
            error("from test | EVAL embedding = TEXT_EMBEDDING(last_name, ?)", defaultAnalyzer, TEXT_EMBEDDING_INFERENCE_ID),
            equalTo("1:30: first argument of [TEXT_EMBEDDING(last_name, ?)] must be a constant, received [last_name]")
        );
    }

    public void testTextEmbeddingFunctionInvalidInferenceId() {
        assertThat(
            error("from test | EVAL embedding = TEXT_EMBEDDING(?, null)", defaultAnalyzer, "query text"),
            equalTo("1:30: second argument of [TEXT_EMBEDDING(?, null)] cannot be null, received [null]")
        );

        assertThat(
            error("from test | EVAL embedding = TEXT_EMBEDDING(?, 42)", defaultAnalyzer, "query text"),
            equalTo("1:30: second argument of [TEXT_EMBEDDING(?, 42)] must be [string], found value [42] type [integer]")
        );

        assertThat(
            error("from test | EVAL embedding = TEXT_EMBEDDING(?, last_name)", defaultAnalyzer, "query text"),
            equalTo("1:30: second argument of [TEXT_EMBEDDING(?, last_name)] must be a constant, received [last_name]")
        );
    }

    public void testInlineStatsInTSNotAllowed() {
        assertThat(error("TS test | INLINE STATS max(network.connections)", tsdb), equalTo("""
            1:11: INLINE STATS [INLINE STATS max(network.connections)] \
            can only be used after STATS when used with TS command"""));

        assertThat(error("""
            TS test |
            INLINE STATS max(network.connections) |
            STATS max(max_over_time(network.connections)) BY host, time_bucket = bucket(@timestamp,1minute)""", tsdb), equalTo("""
            2:1: INLINE STATS [INLINE STATS max(network.connections)] \
            can only be used after STATS when used with TS command"""));

        assertThat(error("TS test | INLINE STATS max_bytes=max(to_long(network.bytes_in)) BY host", tsdb), equalTo("""
            1:11: INLINE STATS [INLINE STATS max_bytes=max(to_long(network.bytes_in)) BY host] \
            can only be used after STATS when used with TS command"""));

        assertThat(error("TS test | INLINE STATS max(60 * rate(network.bytes_in)), max(network.connections)", tsdb), equalTo("""
            1:11: INLINE STATS [INLINE STATS max(60 * rate(network.bytes_in)), max(network.connections)] \
            can only be used after STATS when used with TS command"""));

        assertThat(error("TS test METADATA _tsid | INLINE STATS cnt = count_distinct(_tsid) BY metricset, host", tsdb), equalTo("""
            1:26: INLINE STATS [INLINE STATS cnt = count_distinct(_tsid) BY metricset, host] \
            can only be used after STATS when used with TS command"""));

        assertThat(
            error("""
                TS test |
                INLINE STATS max_cost=max(last_over_time(network.connections)) BY host, time_bucket = bucket(@timestamp,1minute)""", tsdb),
            equalTo("""
                2:1: INLINE STATS [INLINE STATS max_cost=max(last_over_time(network.connections)) \
                BY host, time_bucket = bucket(@timestamp,1minute)] \
                can only be used after STATS when used with TS command""")
        );

        assertThat(
            error("TS test | INLINE STATS max_bytes=max(to_long(network.bytes_in)) BY host | SORT max_bytes DESC | keep max*, host", tsdb),
            equalTo("""
                1:11: INLINE STATS [INLINE STATS max_bytes=max(to_long(network.bytes_in)) BY host] \
                can only be used after STATS when used with TS command""")
        );

        assertThat(error("TS test | INLINE STATS max(network.connections) | STATS max(network.connections) by host", tsdb), equalTo("""
            1:11: INLINE STATS [INLINE STATS max(network.connections)] \
            can only be used after STATS when used with TS command"""));
    }

    public void testInlineStatsWithRateNotAllowed() {
        assertThat(error("TS test | INLINE STATS max(60 * rate(network.bytes_in)), max(network.connections)", tsdb), equalTo("""
            1:11: INLINE STATS [INLINE STATS max(60 * rate(network.bytes_in)), max(network.connections)] \
            can only be used after STATS when used with TS command"""));

    }

    public void testLimitBeforeInlineStats_WithTS() {
        assumeTrue("LIMIT before INLINE STATS limitation check", EsqlCapabilities.Cap.FORBID_LIMIT_BEFORE_INLINE_STATS.isEnabled());
        assertThat(
            error("TS k8s | STATS m=max(network.eth0.tx) BY pod, cluster | LIMIT 5 | INLINE STATS max(m) BY pod"),
            containsString(
                "1:67: INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                    + "but was [INLINE STATS max(m) BY pod] after [LIMIT 5] [@1:57]"
            )
        );
    }

    public void testLimitBeforeInlineStats_WithFork() {
        assumeTrue("LIMIT before INLINE STATS limitation check", EsqlCapabilities.Cap.FORBID_LIMIT_BEFORE_INLINE_STATS.isEnabled());
        assertThat(
            error(
                "FROM test\n"
                    + "| WHERE emp_no == 10048 OR emp_no == 10081\n"
                    + "| FORK (EVAL a = CONCAT(first_name, \" \", emp_no::keyword, \" \", last_name)\n"
                    + "        | GROK a \"%{WORD:x} %{WORD:y} %{WORD:z}\" )\n"
                    + "       (EVAL b = CONCAT(last_name, \" \", emp_no::keyword, \" \", first_name)\n"
                    + "        | GROK b \"%{WORD:x} %{WORD:y} %{WORD:z}\" )\n"
                    + "| SORT _fork, emp_no"
                    + "| INLINE STATS max_lang = MAX(languages) BY gender"
            ),
            containsString(
                "7:23: INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                    + "but was [INLINE STATS max_lang = MAX(languages) BY gender] "
                    + "after [(EVAL a = CONCAT(first_name, \" \", emp_no::keyword, \" \", last_name)\n"
                    + "        | GROK a \"%{WORD:x} %{WORD:y} %{WOR...] [@3:8]"
            )
        );

        assertThat(
            error(
                "FROM test\n"
                    + "| KEEP emp_no, languages, gender\n"
                    + "| FORK (WHERE emp_no == 10048 OR emp_no == 10081)\n"
                    + "       (WHERE emp_no == 10081 OR emp_no == 10087)\n"
                    + "| LIMIT 5"
                    + "| INLINE STATS max_lang = MAX(languages) BY gender \n"
                    + "| SORT emp_no, gender, _fork\n"
            ),
            containsString(
                "5:12: INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                    + "but was [INLINE STATS max_lang = MAX(languages) BY gender] after [LIMIT 5] [@5:3]"
            )
        );

        assertThat(
            error(
                "FROM test\n"
                    + "| KEEP emp_no, languages, gender\n"
                    + "| FORK (WHERE emp_no == 10048 OR emp_no == 10081)\n"
                    + "       (WHERE emp_no == 10081 OR emp_no == 10087)\n"
                    + "| INLINE STATS max_lang = MAX(languages) BY gender \n"
                    + "| SORT emp_no, gender, _fork\n"
                    + "| LIMIT 5"
            ),
            containsString(
                "5:3: INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                    + "but was [INLINE STATS max_lang = MAX(languages) BY gender] "
                    + "after [(WHERE emp_no == 10048 OR emp_no == 10081)\n"
                    + "       (WHERE emp_no == 10081 OR emp_no == 10087)] [@3:8]"
            )
        );
    }

    public void testLimitBeforeInlineStats_WithFrom_And_Row() {
        assumeTrue("LIMIT before INLINE STATS limitation check", EsqlCapabilities.Cap.FORBID_LIMIT_BEFORE_INLINE_STATS.isEnabled());
        var sourceCommands = new String[] { "FROM test | ", "ROW salary=1,gender=\"M\",languages=1 | " };

        assertThat(
            error(randomFrom(sourceCommands) + "LIMIT 5 | INLINE STATS max(salary) BY gender"),
            containsString(
                "INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                    + "but was [INLINE STATS max(salary) BY gender] after [LIMIT 5] [@"
            )
        );

        assertThat(
            error(randomFrom(sourceCommands) + "SORT languages | LIMIT 5 | INLINE STATS max(salary) BY gender"),
            containsString(
                "INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                    + "but was [INLINE STATS max(salary) BY gender] after [LIMIT 5] [@"
            )
        );

        assertThat(
            error(randomFrom(sourceCommands) + "INLINE STATS avg(salary) | LIMIT 5 | INLINE STATS max(salary) BY gender"),
            containsString(
                "INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                    + "but was [INLINE STATS max(salary) BY gender] after [LIMIT 5] [@"
            )
        );

        assertThat(
            error(
                randomFrom(sourceCommands) + "LIMIT 1 | LIMIT 2 | INLINE STATS avg(salary) | LIMIT 5 | INLINE STATS max(salary) BY gender"
            ),
            allOf(
                containsString(
                    "INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                        + "but was [INLINE STATS max(salary) BY gender] after [LIMIT 5] [@"
                ),
                containsString(
                    "INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                        + "but was [INLINE STATS avg(salary)] after [LIMIT 2] [@"
                )
            )
        );

        assertThat(
            error(randomFrom(sourceCommands) + """
                EVAL x = 1
                | LIMIT 1
                | INLINE STATS avg(salary)
                | STATS m=max(languages) BY s=salary/10000
                | LIMIT 5
                | INLINE STATS max(s) BY m
                """),
            allOf(
                containsString(
                    "INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                        + "but was [INLINE STATS max(s) BY m] after [LIMIT 5] [@"
                ),
                containsString(
                    "INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                        + "but was [INLINE STATS avg(salary)] after [LIMIT 1] [@"
                )
            )
        );
    }

    public void testMvExpandBeforeTSStatsNotAllowed() {
        assertThat(error("TS test | MV_EXPAND name | STATS max(network.connections)", tsdb), equalTo("""
            1:11: mv_expand [MV_EXPAND name] in the time-series before the first aggregation \
            [STATS max(network.connections)] is not allowed"""));

        assertThat(error("TS test | MV_EXPAND name | MV_EXPAND network.connections | STATS max(network.connections)", tsdb), equalTo("""
            1:28: mv_expand [MV_EXPAND network.connections] in the time-series before the first aggregation \
            [STATS max(network.connections)] is not allowed
            line 1:11: mv_expand [MV_EXPAND name] in the time-series before the first aggregation \
            [STATS max(network.connections)] is not allowed"""));
    }

    public void testTRangeFailures() {
        assertThat(error("TS test | WHERE TRANGE(-1h) | KEEP @timestamp", tsdb), equalTo("1:24: start_time_or_offset cannot be negative"));

        assertThat(error("TS test | WHERE TRANGE(\"2024-05-10T00:17:14.000Z\") | KEEP @timestamp", tsdb), equalTo("""
            1:17: first argument of [TRANGE("2024-05-10T00:17:14.000Z")] must be [time_duration or date_period], \
            found value ["2024-05-10T00:17:14.000Z"] type [keyword]"""));

        assertThat(error("TS test | WHERE TRANGE(1 hour, 2 hours) | KEEP @timestamp", tsdb), equalTo("""
            1:17: first argument of [TRANGE(1 hour, 2 hours)] must be [string, long, date or date_nanos], \
            found value [1 hour] type [time_duration]"""));

        assertThat(error("TS test | WHERE TRANGE(1715300236000, \"2024-05-10T00:17:14.000Z\") | KEEP @timestamp", tsdb), equalTo("""
            1:17: second argument of [TRANGE(1715300236000, "2024-05-10T00:17:14.000Z")] must be [long], \
            found value ["2024-05-10T00:17:14.000Z"] type [keyword]"""));

        assertThat(error("TS test | WHERE TRANGE(\"2024-05-10T00:17:14.000Z\", 1 hour) | KEEP @timestamp", tsdb), equalTo("""
            1:17: second argument of [TRANGE("2024-05-10T00:17:14.000Z", 1 hour)] must be [keyword], \
            found value [1 hour] type [time_duration]"""));
    }

    /**
     * If there is explicit casting on fields with mix data types between subquery and main index {@code VerificationException} is thrown.
     */
    public void testMixedDataTypesInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String errorMessage = error("""
            FROM test, (FROM test_mixed_types | WHERE languages > 0)
            | WHERE emp_no > 10000
            | SORT is_rehired, still_hired
            """);
        assertThat(errorMessage, containsString("Column [emp_no] has conflicting data types in subqueries: [integer, long]"));
        assertThat(errorMessage, containsString("Column [is_rehired] has conflicting data types in subqueries: [boolean, keyword]"));
        assertThat(errorMessage, containsString("Column [still_hired] has conflicting data types in subqueries: [boolean, keyword]"));
    }

    // Fork inside subquery is tested in LogicalPlanOptimizerTests
    public void testSubqueryInFromWithForkInMainQuery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String errorMessage = error("""
            FROM test, (FROM test_mixed_types
                                 | WHERE languages > 0
                                 | EVAL emp_no = emp_no::int
                                 | KEEP emp_no)
            | FORK (WHERE emp_no > 10000) (WHERE emp_no <= 10000)
            | KEEP emp_no
            """);
        assertThat(errorMessage, containsString("1:6: FORK after subquery is not supported"));
    }

    // LookupJoin on FTF after subquery is not supported, as join is not pushed down into subquery yet
    // FTF on the join(after subquery) on condition is not visible inside subquery yet. FTF after Fork fails with a similar error.
    public void testSubqueryInFromWithLookupJoinOnFullTextFunction() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String errorMessage = error("""
            FROM test, (FROM test_mixed_types
                                 | WHERE languages > 0
                                 | EVAL emp_no = emp_no::int
                                 | KEEP emp_no, languages)
            | LOOKUP JOIN languages_lookup ON languages == language_code AND MATCH(language_name,"English")
            | KEEP emp_no, languages, language_name
            """, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION);
        assertThat(errorMessage, containsString("5:3: [MATCH] function cannot be used after test, (FROM test_mixed_types"));
    }

    public void testChunkFunctionInvalidInputs() {
        assertThat(
            error("from test | EVAL chunks = CHUNK(body, null)", fullTextAnalyzer, VerificationException.class),
            equalTo("1:27: invalid chunking_settings, found [null]")
        );
        assertThat(
            error("from test | EVAL chunks = CHUNK(body, {\"strategy\": \"invalid\"})", fullTextAnalyzer, VerificationException.class),
            equalTo("1:27: Invalid chunkingStrategy invalid")
        );
        assertThat(
            error(
                "from test | EVAL chunks = CHUNK(body, {\"strategy\": \"sentence\", \"max_chunk_size\": 5, \"sentence_overlap\": 1})",
                fullTextAnalyzer,
                VerificationException.class
            ),
            equalTo(
                "1:27: Validation Failed: 1: [chunking_settings] Invalid value [5.0]. "
                    + "[max_chunk_size] must be greater than or equal to [20.0];"
            )
        );
        assertThat(
            error(
                "from test | EVAL chunks = CHUNK(body, {\"strategy\": \"sentence\", \"max_chunk_size\": 5, \"sentence_overlap\": 5})",
                fullTextAnalyzer,
                VerificationException.class
            ),
            equalTo(
                "1:27: Validation Failed: 1: [chunking_settings] Invalid value [5.0]. "
                    + "[max_chunk_size] must be greater than or equal to [20.0];2: sentence_overlap[5] must be either 0 or 1;"
            )
        );
        assertThat(
            error(
                "from test | EVAL chunks = CHUNK(body, {\"strategy\": \"sentence\", \"max_chunk_size\": 20, "
                    + "\"sentence_overlap\": 1, \"extra_value\": \"foo\"})",
                fullTextAnalyzer,
                VerificationException.class
            ),
            equalTo("1:27: Validation Failed: 1: Sentence based chunking settings can not have the following settings: [extra_value];")
        );
    }

    public void testTopSnippetsFunctionInvalidInputs() {
        // Null field allowed
        query("from test | EVAL snippets = TOP_SNIPPETS(null, \"query\")");

        assertThat(
            error("from test | EVAL snippets = TOP_SNIPPETS(42, \"query\")", fullTextAnalyzer, VerificationException.class),
            equalTo("1:29: first argument of [TOP_SNIPPETS(42, \"query\")] must be [string], found value [42] type [integer]")
        );
        assertThat(
            error("from test | EVAL snippets = TOP_SNIPPETS(body, 42)", fullTextAnalyzer, VerificationException.class),
            equalTo("1:29: second argument of [TOP_SNIPPETS(body, 42)] must be [string], found value [42] type [integer]")
        );
        assertThat(
            error("from test | EVAL snippets = TOP_SNIPPETS(body, \"query\", null)", fullTextAnalyzer, VerificationException.class),
            equalTo("1:29: third argument of [TOP_SNIPPETS(body, \"query\", null)] cannot be null, received [null]")
        );
        assertThat(
            error("from test | EVAL snippets = TOP_SNIPPETS(body, \"query\", \"notamap\")", fullTextAnalyzer, VerificationException.class),
            equalTo("1:29: third argument of [TOP_SNIPPETS(body, \"query\", \"notamap\")] must be a map expression, received [\"notamap\"]")
        );
        assertThat(
            error(
                "from test | EVAL snippets = TOP_SNIPPETS(body, \"query\", {\"num_snippets\": null})",
                fullTextAnalyzer,
                ParsingException.class
            ),
            equalTo("1:57: Invalid named parameter [\"num_snippets\":null], NULL is not supported")
        );
        assertThat(
            error(
                "from test | EVAL snippets = TOP_SNIPPETS(body, \"query\", {\"num_words\": null})",
                fullTextAnalyzer,
                ParsingException.class
            ),
            equalTo("1:57: Invalid named parameter [\"num_words\":null], NULL is not supported")
        );
        assertThat(
            error(
                "from test | EVAL snippets = TOP_SNIPPETS(body, \"query\", {\"invalid\": \"foobar\"})",
                fullTextAnalyzer,
                VerificationException.class
            ),
            startsWith("1:29: Invalid option [invalid] in [TOP_SNIPPETS(body, \"query\", {\"invalid\": \"foobar\"})]")
        );
        assertThat(
            error(
                "from test | EVAL snippets = TOP_SNIPPETS(body, \"query\", {\"num_words\": \"foobar\"})",
                fullTextAnalyzer,
                VerificationException.class
            ),
            equalTo(
                "1:29: Invalid option [num_words] in [TOP_SNIPPETS(body, \"query\", {\"num_words\": \"foobar\"})], "
                    + "cannot cast [foobar] to [integer]"
            )
        );
        assertThat(
            error(
                "from test | EVAL snippets = TOP_SNIPPETS(body, \"query\", {\"num_snippets\": \"foobar\"})",
                fullTextAnalyzer,
                VerificationException.class
            ),
            equalTo(
                "1:29: Invalid option [num_snippets] in [TOP_SNIPPETS(body, \"query\", {\"num_snippets\": \"foobar\"})], "
                    + "cannot cast [foobar] to [integer]"
            )
        );
        assertThat(
            error(
                "from test | EVAL snippets = TOP_SNIPPETS(body, \"query\", {\"num_snippets\": -1})",
                fullTextAnalyzer,
                VerificationException.class
            ),
            equalTo("1:29: 'num_snippets' option must be a positive integer, found [-1]")
        );
        assertThat(
            error(
                "from test | EVAL snippets = TOP_SNIPPETS(body, \"query\", {\"num_snippets\": 0})",
                fullTextAnalyzer,
                VerificationException.class
            ),
            equalTo("1:29: 'num_snippets' option must be a positive integer, found [0]")
        );
        assertThat(
            error(
                "from test | EVAL snippets = TOP_SNIPPETS(body, \"query\", {\"num_words\": -1})",
                fullTextAnalyzer,
                VerificationException.class
            ),
            equalTo("1:29: 'num_words' option must be a positive integer, found [-1]")
        );
        assertThat(
            error(
                "from test | EVAL snippets = TOP_SNIPPETS(body, \"query\", {\"num_words\": 0})",
                fullTextAnalyzer,
                VerificationException.class
            ),
            equalTo("1:29: 'num_words' option must be a positive integer, found [0]")
        );
    }

    public void testTimeSeriesWithUnsupportedDataType() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL_WITH_TS_DIMENSIONS.isEnabled());

        // GroupByAll
        assertThat(
            error("TS k8s | STATS rate(network.eth0.tx)", k8s),
            equalTo(
                "1:16: first argument of [rate(network.eth0.tx)] must be [counter_long, counter_integer or counter_double], "
                    + "found value [network.eth0.tx] type [integer]"
            )
        );

        assertThat(
            error("TS k8s | STATS rate(network.eth0.tx) by tbucket(1m)", k8s),
            equalTo(
                "1:16: first argument of [rate(network.eth0.tx)] must be [counter_long, counter_integer or counter_double], "
                    + "found value [network.eth0.tx] type [integer]"
            )
        );

        assertThat(
            error("TS k8s | STATS avg(rate(network.eth0.tx))", k8s),
            equalTo(
                "1:20: first argument of [rate(network.eth0.tx)] must be [counter_long, counter_integer or counter_double], "
                    + "found value [network.eth0.tx] type [integer]"
            )
        );
    }

    public void testMvIntersectionValidatesDataTypesAreEqual() {
        List<Tuple<String, String>> values = List.of(
            new Tuple<>("[\"one\", \"two\", \"three\", \"four\", \"five\"]", "keyword"),
            new Tuple<>("[1, 2, 3, 4, 5]", "integer"),
            new Tuple<>("[1, 2, 3, 4, 5]::long", "long"),
            new Tuple<>("[1.1, 2.2, 3.3, 4.4, 5.5]", "double"),
            new Tuple<>("[false, true, true, false]", "boolean")
        );
        for (int i = 0; i < values.size(); i++) {
            for (int j = 0; j < values.size(); j++) {
                if (i == j) {
                    continue;
                }

                String query = "ROW a = "
                    + values.get(i).v1()
                    + ", b = "
                    + values.get(j).v1()
                    + " | EVAL finalValue = MV_INTERSECTION(a, b)";
                String expected = "second argument of [MV_INTERSECTION(a, b)] must be ["
                    + values.get(i).v2()
                    + "], found value [b] type ["
                    + values.get(j).v2()
                    + "]";
                assertThat(error(query, tsdb), containsString(expected));
            }
        }
    }

    public void testMvUnionValidatesDataTypesAreEqual() {
        List<Tuple<String, String>> values = List.of(
            new Tuple<>("[\"one\", \"two\", \"three\", \"four\", \"five\"]", "keyword"),
            new Tuple<>("[1, 2, 3, 4, 5]", "integer"),
            new Tuple<>("[1, 2, 3, 4, 5]::long", "long"),
            new Tuple<>("[1.1, 2.2, 3.3, 4.4, 5.5]", "double"),
            new Tuple<>("[false, true, true, false]", "boolean")
        );

        for (int i = 0; i < values.size(); i++) {
            for (int j = 0; j < values.size(); j++) {
                if (i == j) {
                    continue;
                }
                String query = "ROW a = " + values.get(i).v1() + ", b = " + values.get(j).v1() + " | EVAL finalValue = MV_UNION(a, b)";
                String expected = "second argument of [MV_UNION(a, b)] must be ["
                    + values.get(i).v2()
                    + "], found value [b] type ["
                    + values.get(j).v2()
                    + "]";
                assertThat(error(query, tsdb), containsString(expected));
            }
        }
    }

    private void checkVectorFunctionsNullArgs(String functionInvocation) throws Exception {
        query("from test | eval similarity = " + functionInvocation, fullTextAnalyzer);
    }

    public void testUnsupportedMetadata() {
        // GroupByAll
        assertThat(
            error("FROM k8s METADATA unknown_field"),
            equalTo("1:1: unresolved metadata fields: [?unknown_field]\nline 1:19: Unresolved metadata pattern [unknown_field]")
        );
    }

    public void testMMRDiversifyFieldIsValid() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        query("row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr on dense_embedding limit 10");

        assertThat(
            error("row dense_embedding=\"hello\" | mmr on dense_embedding limit 10", defaultAnalyzer, VerificationException.class),
            equalTo("1:31: MMR diversify field must be a dense vector field")
        );
    }

    public void testMMRLimitIsValid() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        query("row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr on dense_embedding limit 10");

        assertThat(
            error(
                "row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr on dense_embedding limit -5",
                defaultAnalyzer,
                VerificationException.class
            ),
            equalTo("1:58: MMR limit must be a positive integer")
        );
    }

    public void testMMRResolvedQueryVectorIsValid() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        query(
            "row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr [0.5, 0.4, 0.3, 0.2]::dense_vector on dense_embedding limit 10"
        );

        query("row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr [0.5, 0.4, 0.3, 0.2] on dense_embedding limit 10");
        query("""
            row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector
            | mmr TEXT_EMBEDDING("some text", "some model") on dense_embedding limit 10
            """);

        query("row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr \"7e7e\" on dense_embedding limit 10");
        query("row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr [15, 16, 20] on dense_embedding limit 10");
    }

    public void testMMRLambdaValueIsValid() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        query("row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr on dense_embedding limit 10 with { \"lambda\": 0.5 }");

        assertThat(
            error(
                "row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr on dense_embedding limit 10 with { \"unknown\": true }",
                defaultAnalyzer,
                VerificationException.class
            ),
            equalTo("1:58: Invalid option [unknown] in <MMR>, expected one of [[lambda]]")
        );

        assertThat(
            error(
                "row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr on dense_embedding limit 10 with "
                    + "{ \"lambda\": 0.5, \"unknown_extra\": true }",
                defaultAnalyzer,
                VerificationException.class
            ),
            equalTo("1:58: Invalid option [unknown_extra] in <MMR>, expected one of [[lambda]]")
        );

        assertThat(
            error(
                "row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr on dense_embedding limit 10 with { \"lambda\": 2.5 }",
                defaultAnalyzer,
                VerificationException.class
            ),
            equalTo("1:58: MMR lambda value must be a number between 0.0 and 1.0")
        );
        assertThat(
            error(
                "row dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector | mmr on dense_embedding limit 10 with { \"lambda\": -2.5 }",
                defaultAnalyzer,
                VerificationException.class
            ),
            equalTo("1:58: MMR lambda value must be a number between 0.0 and 1.0")
        );
    }

    public void testMMRLimitedInput() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        assertThat(error("""
            FROM test
            | EVAL dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector
            | MMR ON dense_embedding LIMIT 10
            """), containsString("MMR can only be used on a limited number of rows. Consider adding a LIMIT before MMR."));

        assertThat(error("""
            FROM (FROM test METADATA _index, _id, _score | EVAL dense_embedding=[0.5, 0.4, 0.3, 0.2]::dense_vector),
                 (FROM test METADATA _index, _id, _score | LIMIT 10)
            | MMR ON dense_embedding LIMIT 10
            """), containsString("MMR can only be used on a limited number of rows. Consider adding a LIMIT before MMR."));
    }

    private void query(String query) {
        query(query, defaultAnalyzer);
    }

    private void query(String query, Analyzer analyzer) {
        analyzer.analyze(EsqlParser.INSTANCE.parseQuery(query));
    }

    private String error(String query) {
        return error(query, defaultAnalyzer);
    }

    private String error(String query, Object... params) {
        return error(query, defaultAnalyzer, VerificationException.class, params);
    }

    public static String error(String query, Analyzer analyzer, Object... params) {
        return error(query, analyzer, VerificationException.class, params);
    }

    private String error(String query, TransportVersion transportVersion, Object... params) {
        return error(query, transportVersion, VerificationException.class, params);
    }

    public static String error(String query, Analyzer analyzer, Class<? extends Exception> exception, Object... params) {
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
            () -> analyzer.analyze(EsqlParser.INSTANCE.parseQuery(query, new QueryParams(parameters)))
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

    private String error(String query, TransportVersion transportVersion, Class<? extends Exception> exception, Object... params) {
        MutableAnalyzerContext mutableContext = (MutableAnalyzerContext) defaultAnalyzer.context();
        try (var restore = mutableContext.setTemporaryTransportVersionOnOrAfter(transportVersion)) {
            return error(query, defaultAnalyzer, exception, params);
        }
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
