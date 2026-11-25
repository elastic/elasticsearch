/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.fieldCapabilitiesIndexResponse;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.fieldResponseMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * A cast of a field mapped to several types is lenient on the types it doesn't convert: the field is {@code null} in the indices that
 * map them, and the query warns about it. Below {@link ConvertFunction#LENIENT_CAST} the query fails with the error it always had.
 * Each test runs at the current version and at a random older one.
 */
public class LenientUnionCastAnalyzerTests extends AnalyzerTestCase {

    public LenientUnionCastAnalyzerTests(VersionMode versionMode) {
        super(versionMode);
    }

    private boolean lenient() {
        return minimumVersion.supports(ConvertFunction.LENIENT_CAST);
    }

    /**
     * {@code value} is mapped as {@code type1} in {@code foo}, and as {@code type2} in {@code bar}.
     */
    private TestAnalyzer analyzerWithUnion(String type1, String type2) {
        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("foo", fieldResponseMap("value", type1)),
                fieldCapabilitiesIndexResponse("bar", fieldResponseMap("value", type2))
            ),
            List.of()
        );
        // The fields are resolved as the nodes of the version would, which keep the types they don't support only from some version.
        IndexResolution resolution = IndexResolver.mergedMappings(
            "foo,bar",
            false,
            new IndexResolver.FieldsInfo(caps, minimumVersion, false, false, false, false, true, true, true, null, true),
            false,
            IndexResolver.DO_NOT_GROUP
        );
        return analyzer().addIndex(resolution).stripErrorPrefix(true);
    }

    /**
     * A type ES|QL doesn't support is a type of the union that the cast doesn't convert: the field is null for it, and the warning
     * names the type as the index has it. Below {@link ConvertFunction#LENIENT_CAST} the field is unsupported, as ever.
     */
    public void testUnsupportedTypeOfTheUnion() {
        TestAnalyzer analyzer = analyzerWithUnion("long", "rank_feature");
        String query = "FROM foo, bar | EVAL x = value::keyword | KEEP x | LIMIT 1";
        if (lenient() == false) {
            analyzer.error(query, containsString("Cannot use field [value] with unsupported type [rank_feature]"));
            return;
        }
        LogicalPlan plan = analyzer.query(query);
        assertTrue(plan.resolved());
        assertThat(plan.output().getLast().dataType(), equalTo(DataType.KEYWORD));
        assertWarnings(warning("rank_feature", "bar", "KEYWORD"));
    }

    /**
     * A field with an unsupported type is as unsupported as it was when it wasn't kept, with the same message, if it isn't cast.
     */
    public void testUnsupportedTypeOfTheUnionWithoutCast() {
        analyzerWithUnion("long", "rank_feature").error(
            "FROM foo, bar | EVAL x = value + 1 | LIMIT 1",
            containsString("Cannot use field [value] with unsupported type [rank_feature]")
        );
        analyzerWithUnion("rank_feature", "long").error(
            "FROM foo, bar | KEEP value | EVAL x = value + 1 | LIMIT 1",
            containsString("Cannot use field [value] with unsupported type [rank_feature]")
        );
    }

    /**
     * An unsupported type is not one of the types of the field that {@code TO_GAUGE} leaves alone.
     */
    public void testUnsupportedTypeOfTheUnionToGaugeStaysStrict() {
        analyzerWithUnion("long", "rank_feature").error(
            "FROM foo, bar | EVAL x = TO_GAUGE(value) | LIMIT 1",
            containsString("Cannot use field [value] with unsupported type [rank_feature]")
        );
    }

    public void testEveryTypeUnsupported() {
        analyzerWithUnion("rank_feature", "rank_feature").error(
            "FROM foo, bar | EVAL x = value::keyword | LIMIT 1",
            containsString("Cannot use field [value] with unsupported type [rank_feature]")
        );
    }

    /**
     * With a base, {@code TO_LONG} and {@code TO_INTEGER} only parse strings, so they aren't lenient on the types of a union: a union
     * of keyword and text keeps the base, and any other type of the union is an error, as it is when every node has the lenient casts.
     */
    public void testConversionWithBaseStaysStrict() {
        for (String function : List.of("TO_LONG", "TO_INTEGER")) {
            String query = "FROM foo, bar | EVAL x = " + function + "(value, 16) | KEEP x | LIMIT 1";

            LogicalPlan plan = analyzerWithUnion("keyword", "text").query(query);
            assertTrue(plan.resolved());
            assertThat(plan.output().getLast().dataType(), equalTo(function.equals("TO_LONG") ? DataType.LONG : DataType.INTEGER));

            analyzerWithUnion("keyword", "long").error(query, ambiguous());
            analyzerWithUnion("keyword", "rank_feature").error(
                query,
                containsString("Cannot use field [value] with unsupported type [rank_feature]")
            );
        }
    }

    private static Matcher<String> ambiguous() {
        return containsString("Cannot use field [value] due to ambiguities being mapped as [2] incompatible types");
    }

    /**
     * The warning of a field whose type in {@code index} the cast doesn't convert.
     */
    private static String warning(String type, String index, String target) {
        return "Field [value] of type ["
            + type
            + "] in ["
            + index
            + "] cannot be converted to ["
            + target
            + "] and will be null in those indices.";
    }

    public void testTypeTheCastDoesNotAccept() {
        TestAnalyzer analyzer = analyzerWithUnion("ip", "keyword");
        String query = "FROM foo, bar | EVAL x = value::long | KEEP x | LIMIT 1";
        if (lenient() == false) {
            analyzer.error(query, ambiguous());
            return;
        }
        LogicalPlan plan = analyzer.query(query);
        assertTrue(plan.resolved());
        assertThat(plan.output().getLast().dataType(), equalTo(DataType.LONG));
        assertWarnings(warning("ip", "foo", "LONG"));
    }

    public void testEveryTypeTheCastDoesNotAccept() {
        TestAnalyzer analyzer = analyzerWithUnion("ip", "boolean");
        String query = "FROM foo, bar | EVAL x = TO_RADIANS(value) | KEEP x | LIMIT 1";
        if (lenient() == false) {
            analyzer.error(query, ambiguous());
            return;
        }
        LogicalPlan plan = analyzer.query(query);
        assertTrue(plan.resolved());
        assertThat(plan.output().getLast().dataType(), equalTo(DataType.DOUBLE));
        assertWarnings(warning("ip", "foo", "DOUBLE"), warning("boolean", "bar", "DOUBLE"));
    }

    public void testSurrogateConversionOfTypeItDoesNotAccept() {
        TestAnalyzer analyzer = analyzerWithUnion("ip", "keyword");
        String query = "FROM foo, bar | EVAL x = TO_INTEGER(value) | KEEP x | LIMIT 1";
        if (lenient() == false) {
            analyzer.error(query, ambiguous());
            return;
        }
        assertTrue(analyzer.query(query).resolved());
        assertWarnings(warning("ip", "foo", "INTEGER"));
    }

    /**
     * The legs that don't convert are conversions over the type they have: the field can be read from the type it has in each index.
     */
    public void testLegsOfAUnionAreAConversionPerType() {
        if (lenient() == false) {
            return;
        }
        LogicalPlan plan = analyzerWithUnion("ip", "keyword").query("FROM foo, bar | EVAL x = value::long | LIMIT 1");
        List<UnionTypeEsField> unions = new ArrayList<>();
        plan.forEachDown(p -> p.forEachExpression(FieldAttribute.class, fa -> {
            if (fa.field() instanceof UnionTypeEsField union) {
                unions.add(union);
            }
        }));
        assertFalse("expected the union field in the plan", unions.isEmpty());
        for (UnionTypeEsField union : unions) {
            assertThat(union.getConversionExpressions().size(), equalTo(2));
        }
        assertWarnings(warning("ip", "foo", "LONG"));
    }

    /**
     * The per-type conversions of a union are sent to the data nodes, and surrogates like {@code TO_LONG} and {@code TO_IP} can't be
     * serialized, so they are replaced also for the type the conversion doesn't accept, which is unsupported here.
     */
    public void testSurrogateConversionsOfUnsupportedTypeAreSubstituted() {
        if (lenient() == false) {
            return;
        }
        for (String[] conversion : new String[][] {
            { "TO_LONG(value)", "LONG" },
            { "TO_INTEGER(value)", "INTEGER" },
            { "TO_IP(value)", "IP" },
            { "TO_IP(value, {\"leading_zeros\": \"octal\"})", "IP" } }) {
            LogicalPlan plan = analyzerWithUnion("keyword", "rank_feature").query(
                "FROM foo, bar | EVAL x = " + conversion[0] + " | KEEP x | LIMIT 1"
            );
            List<UnionTypeEsField> unions = new ArrayList<>();
            plan.forEachDown(p -> p.forEachExpression(FieldAttribute.class, fa -> {
                if (fa.field() instanceof UnionTypeEsField union) {
                    unions.add(union);
                }
            }));
            assertFalse("expected the union field in the plan for " + conversion[0], unions.isEmpty());
            for (UnionTypeEsField union : unions) {
                assertThat(union.getConversionExpressions().size(), equalTo(2));
                for (Expression expression : union.getConversionExpressions()) {
                    assertThat(expression.toString(), expression, not(instanceOf(SurrogateExpression.class)));
                }
            }
            assertWarnings(warning("rank_feature", "bar", conversion[1]));
        }
    }

    public void testNoWarningWhenEveryTypeConverts() {
        TestAnalyzer analyzer = analyzerWithUnion("keyword", "long");
        assertTrue(analyzer.query("FROM foo, bar | EVAL x = value::long | KEEP x | LIMIT 1").resolved());
        assertTrue(analyzer.query("FROM foo, bar | EVAL x = value::keyword | KEEP x | LIMIT 1").resolved());
    }

    /**
     * A conversion that is null for a type, and needs the type of its input or a constant, is still an error.
     */
    public void testConversionsThatStayStrict() {
        TestAnalyzer analyzer = analyzerWithUnion("ip", "keyword");
        analyzer.error("FROM foo, bar | EVAL x = TO_GAUGE(value) | LIMIT 1", ambiguous());
        analyzer.error("FROM foo, bar | EVAL x = TO_COUNTER(value) | LIMIT 1", ambiguous());
        analyzer.error("FROM foo, bar | EVAL x = TO_LONG(value, 16) | LIMIT 1", ambiguous());
        analyzer.error("FROM foo, bar | EVAL x = TO_INTEGER(value, 16) | LIMIT 1", ambiguous());
        analyzer.error("FROM foo, bar | EVAL x = TO_DATEPERIOD(value) | LIMIT 1", containsString("must be a constant"));
    }

    /**
     * Only the types the cast converts are lenient: other arguments are still checked.
     */
    public void testOptionsAreStillChecked() {
        // TO_IP doesn't accept a long: without lenient casts that union is rejected before the options are checked
        analyzerWithUnion(lenient() ? "long" : "ip", "keyword").error(
            "FROM foo, bar | EVAL x = TO_IP(value, {\"unknown\": \"x\"}) | LIMIT 1",
            containsString("Invalid option [unknown]")
        );
    }
}
