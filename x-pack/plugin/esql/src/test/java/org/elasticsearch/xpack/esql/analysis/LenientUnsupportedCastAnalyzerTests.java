/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * A cast of a field of a type ES|QL doesn't support is {@code null} with a warning that names the type, instead of failing, when every
 * node supports it ({@link ConvertFunction#LENIENT_CAST}). Below that version it fails with the error it always had. The field is
 * still unsupported where it's not cast. Each test runs at the current version and at a random older one.
 */
public class LenientUnsupportedCastAnalyzerTests extends AnalyzerTestCase {

    public LenientUnsupportedCastAnalyzerTests(VersionMode versionMode) {
        super(versionMode);
    }

    private boolean lenient() {
        return minimumVersion.supports(ConvertFunction.LENIENT_CAST);
    }

    private TestAnalyzer analyzerWithUnsupported() {
        EsField unsupported = new UnsupportedEsField("unsupported", List.of("rank_feature"));
        EsField number = new EsField("number", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        return analyzer().addIndex(
            "test*",
            IndexResolution.valid(EsIndexGenerator.esIndex("test*", Map.of("unsupported", unsupported, "number", number)))
        ).stripErrorPrefix(true);
    }

    private static String warning(String field, String target) {
        return "[" + field + "] of type [rank_feature] cannot be converted to [" + target + "] and will be null.";
    }

    private static Matcher<String> unsupportedError() {
        return containsString("Cannot use field [unsupported] with unsupported type [rank_feature]");
    }

    public void testCastOfUnsupportedField() {
        String query = "FROM test* | EVAL x = unsupported::long | KEEP x | LIMIT 1";
        if (lenient() == false) {
            analyzerWithUnsupported().error(query, unsupportedError());
            return;
        }
        LogicalPlan plan = analyzerWithUnsupported().query(query);
        assertTrue(plan.resolved());
        assertThat(plan.output().getLast().dataType(), equalTo(DataType.LONG));
        assertWarnings(warning("unsupported", "LONG"));
    }

    public void testConversionFunctionOfUnsupportedField() {
        String query = "FROM test* | EVAL x = TO_STRING(unsupported) | KEEP x | LIMIT 1";
        if (lenient() == false) {
            analyzerWithUnsupported().error(query, unsupportedError());
            return;
        }
        LogicalPlan plan = analyzerWithUnsupported().query(query);
        assertTrue(plan.resolved());
        assertThat(plan.output().getLast().dataType(), equalTo(DataType.KEYWORD));
        assertWarnings(warning("unsupported", "KEYWORD"));
    }

    /**
     * The field is unsupported wherever it's not the input of a conversion, as it always was: it's still returned as unsupported, and
     * using it fails.
     */
    public void testFieldIsStillUnsupportedWithoutCast() {
        LogicalPlan plan = analyzerWithUnsupported().query("FROM test* | KEEP unsupported | LIMIT 1");
        assertThat(plan.output().getLast().dataType(), equalTo(DataType.UNSUPPORTED));
        analyzerWithUnsupported().error("FROM test* | EVAL x = unsupported + 1 | LIMIT 1", unsupportedError());
        analyzerWithUnsupported().error("FROM test* | EVAL x = LENGTH(unsupported) | LIMIT 1", unsupportedError());
    }

    /**
     * Casting the field in one place doesn't make it usable in the others, nor changes what it is in the output.
     */
    public void testCastAndUnsupportedFieldTogether() {
        String query = "FROM test* | EVAL x = unsupported::long | KEEP unsupported, x | LIMIT 1";
        if (lenient() == false) {
            analyzerWithUnsupported().error(query, unsupportedError());
            return;
        }
        LogicalPlan plan = analyzerWithUnsupported().query(query);
        List<Attribute> output = plan.output();
        assertThat(output.get(0).name(), equalTo("unsupported"));
        assertThat(output.get(0).dataType(), equalTo(DataType.UNSUPPORTED));
        assertThat(output.get(1).dataType(), equalTo(DataType.LONG));
        assertWarnings(warning("unsupported", "LONG"));
    }

    /**
     * The cast stays over a reference to the field: it's not a constant, so it must still appear in the {@code BY} of a {@code STATS}.
     */
    public void testCastOfUnsupportedFieldInStats() {
        if (lenient() == false) {
            return;
        }
        analyzerWithUnsupported().error(
            "FROM test* | STATS x = unsupported::long | LIMIT 1",
            containsString("column [unsupported] must appear in the STATS BY clause or be used in an aggregate function")
        );
        LogicalPlan plan = analyzerWithUnsupported().query("FROM test* | STATS c = COUNT(unsupported::keyword) | LIMIT 1");
        assertTrue(plan.resolved());
        assertWarnings(warning("unsupported", "KEYWORD"));
    }

    /**
     * Other arguments of the conversion are still checked.
     */
    public void testOtherArgumentsAreStillChecked() {
        if (lenient() == false) {
            return;
        }
        analyzerWithUnsupported().error(
            "FROM test* | EVAL x = TO_IP(unsupported, {\"unknown\": \"x\"}) | LIMIT 1",
            containsString("Invalid option [unknown]")
        );
    }

    /**
     * The conversions that can't be {@code null} for an input they don't convert stay strict.
     */
    public void testConversionsThatStayStrict() {
        analyzerWithUnsupported().error("FROM test* | EVAL x = TO_GAUGE(unsupported) | LIMIT 1", unsupportedError());
        analyzerWithUnsupported().error("FROM test* | EVAL x = TO_COUNTER(unsupported) | LIMIT 1", unsupportedError());
        analyzerWithUnsupported().error("FROM test* | EVAL x = TO_LONG(unsupported, 16) | LIMIT 1", unsupportedError());
    }

    /**
     * The branches of a {@code FORK} are analyzed again when their outputs are aligned, so the cast of the field must settle.
     */
    public void testCastInForkBranch() {
        if (lenient() == false) {
            return;
        }
        LogicalPlan plan = analyzerWithUnsupported().query(
            "FROM test* | FORK (EVAL y = unsupported::long | WHERE number > 0) (EVAL y = 1::long) | KEEP y | LIMIT 5"
        );
        assertTrue(plan.resolved());
        assertThat(plan.output().getLast().dataType(), equalTo(DataType.LONG));
        assertWarnings(warning("unsupported", "LONG"));
    }

    /**
     * The reference to the field that the cast is over keeps the id of the field, so the plan has no other column with its name.
     */
    public void testCastKeepsTheIdOfTheField() {
        if (lenient() == false) {
            return;
        }
        LogicalPlan plan = analyzerWithUnsupported().query("FROM test* | EVAL x = unsupported::long | KEEP unsupported, x | LIMIT 1");
        Attribute unsupported = plan.output().getFirst();
        plan.forEachDown(p -> p.forEachExpression(FieldAttribute.class, fa -> {
            if (fa.name().equals("unsupported")) {
                assertThat(fa.id(), equalTo(unsupported.id()));
            }
        }));
        assertWarnings(warning("unsupported", "LONG"));
    }
}
