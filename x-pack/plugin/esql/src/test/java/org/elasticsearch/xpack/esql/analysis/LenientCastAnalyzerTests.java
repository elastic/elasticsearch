/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * A cast, or a type conversion function, whose input has a single type it can't convert is {@code null} with a warning instead of
 * failing, when every node supports it ({@link ConvertFunction#LENIENT_CAST}). Below that version it fails with the error it always
 * had. Each test runs at the current version and at a random older one.
 * <p>
 * Casts of union fields and of fields of an unsupported type are not covered here: they keep failing until the changes that build on
 * this one.
 * </p>
 */
public class LenientCastAnalyzerTests extends AnalyzerTestCase {

    public LenientCastAnalyzerTests(VersionMode versionMode) {
        super(versionMode);
    }

    private boolean lenient() {
        return minimumVersion.supports(ConvertFunction.LENIENT_CAST);
    }

    private TestAnalyzer defaultAnalyzer() {
        return analyzer().addDefaultIndex().stripErrorPrefix(true);
    }

    /**
     * The error of a call whose argument has a type it doesn't accept.
     */
    private static Matcher<String> invalidInput(String call, String value, String type) {
        return allOf(
            containsString("argument of [" + call + "] must be ["),
            containsString("], found value [" + value + "] type [" + type + "]")
        );
    }

    /**
     * The query is {@code null} of {@code type} with a warning when conversions are lenient, and fails with the error it always had
     * otherwise.
     *
     * @param call the call that can't convert its input, as written in the error
     * @param value the input of the call, as it is named in the error and the warning
     * @param inputType the type of the input
     */
    private void assertLenientCast(String query, DataType type, String call, String value, DataType inputType) {
        TestAnalyzer analyzer = defaultAnalyzer();
        if (lenient() == false) {
            analyzer.error(query, invalidInput(call, value, inputType.typeName()));
            return;
        }
        LogicalPlan plan = analyzer.query(query);
        assertTrue("expected a resolved plan for [" + query + "]", plan.resolved());
        assertThat(plan.output().getLast().dataType(), equalTo(type));
        assertWarnings(
            "[" + value + "] of type [" + inputType.typeName() + "] cannot be converted to [" + type.nameUpper() + "] and will be null."
        );
    }

    public void testFieldOfTypeTheCastDoesNotAccept() {
        assertLenientCast("FROM test | EVAL x = salary::ip | KEEP x | LIMIT 1", IP, "salary::ip", "salary", INTEGER);
    }

    public void testExpressionOfTypeTheCastDoesNotAccept() {
        assertLenientCast("FROM test | EVAL x = (salary + 1)::ip | KEEP x | LIMIT 1", IP, "(salary + 1)::ip", "salary + 1", INTEGER);
    }

    public void testLiteralOfTypeTheCastDoesNotAccept() {
        assertLenientCast("ROW x = true::ip | LIMIT 1", IP, "true::ip", "true", DataType.BOOLEAN);
    }

    public void testConversionFunctionsOfTypeTheyDoNotAccept() {
        assertLenientCast(
            "FROM test | EVAL x = TO_RADIANS(still_hired) | KEEP x | LIMIT 1",
            DOUBLE,
            "TO_RADIANS(still_hired)",
            "still_hired",
            DataType.BOOLEAN
        );
        assertLenientCast(
            "FROM test | EVAL x = TO_DEGREES(still_hired) | KEEP x | LIMIT 1",
            DOUBLE,
            "TO_DEGREES(still_hired)",
            "still_hired",
            DataType.BOOLEAN
        );
    }

    /**
     * {@code TO_LONG} and {@code TO_INTEGER} are surrogates that resolve through the conversion they become.
     */
    public void testSurrogateConversionsOfTypeTheyDoNotAccept() {
        assertLenientCast("ROW ip = TO_IP(\"1.2.3.4\") | EVAL x = TO_LONG(ip) | KEEP x | LIMIT 1", LONG, "TO_LONG(ip)", "ip", IP);
        assertLenientCast("ROW ip = TO_IP(\"1.2.3.4\") | EVAL x = TO_INTEGER(ip) | KEEP x | LIMIT 1", INTEGER, "TO_INTEGER(ip)", "ip", IP);
    }

    /**
     * Only the type of the input is lenient: a conversion with invalid options keeps failing.
     */
    public void testOptionsAreStillChecked() {
        defaultAnalyzer().error(
            "FROM test | EVAL x = TO_IP(first_name, {\"unknown\": \"x\"}) | LIMIT 1",
            equalTo("1:22: Invalid option [unknown] in [TO_IP(first_name, {\"unknown\": \"x\"})], expected one of [leading_zeros]")
        );
        assertLenientCast(
            "FROM test | EVAL x = TO_IP(salary, {\"leading_zeros\": \"octal\"}) | KEEP x | LIMIT 1",
            IP,
            "TO_IP(salary, {\"leading_zeros\": \"octal\"})",
            "salary",
            INTEGER
        );
        // The options are invalid and so is the input: the error is the one of the input, as it always was.
        defaultAnalyzer().error(
            "FROM test | EVAL x = TO_IP(salary, {\"unknown\": \"x\"}) | LIMIT 1",
            invalidInput("TO_IP(salary, {\"unknown\": \"x\"})", "salary", "integer")
        );
    }

    /**
     * Conversions whose result depends on the type of their input, that need a constant, or that parse strings keep failing.
     */
    public void testConversionsThatStayStrict() {
        defaultAnalyzer().error(
            "FROM test | EVAL x = TO_GAUGE(first_name) | LIMIT 1",
            invalidInput("TO_GAUGE(first_name)", "first_name", "keyword")
        );
        defaultAnalyzer().error(
            "FROM test | EVAL x = TO_COUNTER(first_name) | LIMIT 1",
            invalidInput("TO_COUNTER(first_name)", "first_name", "keyword")
        );
        defaultAnalyzer().error(
            "FROM test | EVAL x = TO_LONG(salary, 16) | LIMIT 1",
            invalidInput("TO_LONG(salary, 16)", "salary", "integer")
        );
        defaultAnalyzer().error(
            "FROM test | EVAL x = TO_INTEGER(salary, 16) | LIMIT 1",
            invalidInput("TO_INTEGER(salary, 16)", "salary", "integer")
        );
        defaultAnalyzer().error(
            "FROM test | EVAL x = TO_DATEPERIOD(salary) | LIMIT 1",
            invalidInput("TO_DATEPERIOD(salary)", "salary", "integer")
        );
        defaultAnalyzer().error(
            "FROM test | EVAL x = TO_TIMEDURATION(salary) | LIMIT 1",
            invalidInput("TO_TIMEDURATION(salary)", "salary", "integer")
        );
    }

    /**
     * A type a query can't return stays an error: there is nothing a {@code null} of it could be compared with.
     */
    public void testInputsThatCannotBeReturnedStayStrict() {
        defaultAnalyzer().error("ROW x = TO_STRING(1 year) | LIMIT 1", invalidInput("TO_STRING(1 year)", "1 year", "date_period"));
    }

    /**
     * Inputs that convert, and a {@code null} input, don't warn.
     */
    public void testNoWarningWhenTheInputConverts() {
        TestAnalyzer analyzer = defaultAnalyzer();
        // ImplicitCasting still turns the keyword literal into a double.
        LogicalPlan plan = analyzer.query("ROW x = TO_RADIANS(\"1.5\") | LIMIT 1");
        assertTrue(plan.resolved());
        assertThat(plan.output().getLast().dataType(), equalTo(DOUBLE));
        analyzer.query("ROW x = null::ip | LIMIT 1");
        analyzer.query("FROM test | EVAL x = first_name::ip | KEEP x | LIMIT 1");
        analyzer.query("FROM test | EVAL x = salary::long | KEEP x | LIMIT 1");
    }

    /**
     * Fields of an unsupported type are not an input type the cast can be lenient about. Fields mapped to several types are covered by
     * {@link LenientUnionCastAnalyzerTests}.
     */
    public void testUnsupportedFieldsStayStrict() {
        EsField unsupported = new UnsupportedEsField("unsupported", List.of("flattened"));
        TestAnalyzer analyzer = analyzer().addIndex(
            "test*",
            IndexResolution.valid(EsIndexGenerator.esIndex("test*", Map.of("unsupported", unsupported)))
        ).stripErrorPrefix(true);
        analyzer.error(
            "FROM test* | EVAL x = unsupported::long | LIMIT 1",
            containsString("Cannot use field [unsupported] with unsupported type [flattened]")
        );
    }

    /**
     * A cast is not removed from the plan: aggregates and groupings are checked as for any other expression.
     */
    public void testCastsInStats() {
        assertLenientCast("FROM test | STATS x = COUNT(*)::ip | LIMIT 1", IP, "COUNT(*)::ip", "COUNT(*)", LONG);
        assertLenientCast("FROM test | STATS c = COUNT(*) BY g = salary::ip | LIMIT 1", IP, "salary::ip", "salary", INTEGER);
        defaultAnalyzer().error(
            "FROM test | STATS x = salary::ip | LIMIT 1",
            lenient()
                ? containsString("column [salary] must appear in the STATS BY clause or be used in an aggregate function")
                : invalidInput("salary::ip", "salary", "integer")
        );
    }
}
