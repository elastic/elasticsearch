/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FoldablesConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIntegerBase;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosOctal;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLongBase;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.fieldCapabilitiesIndexResponse;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.fieldResponseMap;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.mergedResolution;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Runs every conversion function registered in ES|QL over union types, so that a change to how unions are resolved can't silently
 * break one of them. The conversions of a union are serialized with the field to the data nodes and run while loading it, so for each
 * union of two types a function accepts, the analysis has to succeed and every per-type conversion has to survive serialization.
 */
public class AnalyzerUnionTypesConversionFunctionsTests extends AnalyzerTestCase {

    /**
     * The types that can be unioned here: the ones a field can be mapped as, and that need nothing besides their name in the field
     * capabilities.
     */
    private static final Set<DataType> UNIONABLE_TYPES = Set.of(
        DataType.BOOLEAN,
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.BYTE,
        DataType.SHORT,
        DataType.INTEGER,
        DataType.LONG,
        DataType.UNSIGNED_LONG,
        DataType.FLOAT,
        DataType.HALF_FLOAT,
        DataType.DOUBLE,
        DataType.DATETIME,
        DataType.DATE_NANOS,
        DataType.IP,
        DataType.VERSION,
        DataType.GEO_POINT,
        DataType.CARTESIAN_POINT,
        DataType.GEO_SHAPE,
        DataType.CARTESIAN_SHAPE,
        DataType.HISTOGRAM,
        DataType.EXPONENTIAL_HISTOGRAM,
        DataType.TDIGEST
    );

    /**
     * The conversion functions that take other arguments besides the field, with those arguments, the conversion each per-type leg of
     * the union has to be, and the pairs of types worth trying them with. If the analyzer dropped the arguments, the legs would be
     * a different conversion. Conversions with optional arguments have to be here, so a new one can't be left untested.
     */
    private record WithOtherArguments(
        String function,
        String otherArguments,
        Class<? extends Expression> conversion,
        List<List<DataType>> unions
    ) {}

    private static final List<WithOtherArguments> WITH_OTHER_ARGUMENTS = List.of(
        new WithOtherArguments("to_long", "16", ToLongBase.class, List.of(List.of(DataType.KEYWORD, DataType.TEXT))),
        new WithOtherArguments("to_integer", "16", ToIntegerBase.class, List.of(List.of(DataType.KEYWORD, DataType.TEXT))),
        new WithOtherArguments("to_long", "8 + 8", ToLongBase.class, List.of(List.of(DataType.KEYWORD, DataType.TEXT))),
        new WithOtherArguments(
            "to_ip",
            "{\"leading_zeros\": \"octal\"}",
            ToIpLeadingZerosOctal.class,
            List.of(List.of(DataType.IP, DataType.KEYWORD), List.of(DataType.KEYWORD, DataType.TEXT))
        )
    );

    /**
     * Functions that annotate a field with a metric type instead of converting its values, so their output type follows the input
     * type and a union of different input types can't resolve to one type, whatever the arguments.
     */
    private static final Map<String, String> NOT_CONVERTING_VALUES = Map.of(
        "to_counter",
        "the counter type depends on the input type, so the union of different types has no single output type",
        "to_gauge",
        "the output type is the input type, so the union of different types is a type conflict"
    );

    /** The conversion functions with optional arguments that aren't used on unions, and why. */
    private static final Map<String, String> OPTIONAL_ARGUMENTS_NOT_ON_UNIONS = Map.of(
        "to_text",
        "its analyzer option is rejected on any index-mapped field, as it would re-analyze the values row by row, so it never applies to a union"
    );

    public AnalyzerUnionTypesConversionFunctionsTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    /**
     * Every conversion function that takes a field resolves a union of each two consecutive types it accepts, and the conversions
     * stored in the union are serializable.
     */
    public void testEveryConversionFunctionResolvesUnionTypes() {
        int functionsTested = 0;
        List<String> failures = new ArrayList<>();
        Set<String> functionsWithoutUnion = new TreeSet<>();
        for (FunctionDefinition definition : TEST_FUNCTION_REGISTRY.listFunctions()) {
            if (isConversionOfFields(definition) == false || NOT_CONVERTING_VALUES.containsKey(definition.name())) {
                continue;
            }
            List<DataType> accepted = acceptedUnionableTypes(definition);
            if (accepted.size() < 2) {
                functionsWithoutUnion.add(definition.name());
                continue;
            }
            functionsTested++;
            // Each type is in at least one union: every type with the next one, and the last one with the first
            Set<List<DataType>> unions = new LinkedHashSet<>();
            for (int i = 0; i < accepted.size(); i++) {
                unions.add(List.of(accepted.get(i), accepted.get((i + 1) % accepted.size())));
            }
            // And every type with some common ones, as the small numeric types and the strings are converted differently
            for (DataType type : accepted) {
                for (DataType partner : List.of(DataType.KEYWORD, DataType.INTEGER, DataType.LONG, DataType.DOUBLE)) {
                    if (partner != type && accepted.contains(partner)) {
                        unions.add(List.of(type, partner));
                    }
                }
            }
            for (List<DataType> union : unions) {
                String failure = unionResolutionFailure(definition.name(), "", null, union.get(0), union.get(1));
                if (failure != null) {
                    failures.add(failure);
                }
            }
        }
        assertThat("some conversion functions should have been tested", functionsTested, not(equalTo(0)));
        assertThat(String.join("\n", failures), failures, empty());
        assertThat(
            "conversion functions that accept fewer than two of the types in UNIONABLE_TYPES, so they can't be tested over a union",
            functionsWithoutUnion,
            empty()
        );
    }

    /**
     * The conversion functions that take other arguments keep them in each per-type conversion of a union.
     */
    public void testConversionFunctionsWithOtherArgumentsResolveUnionTypes() {
        for (WithOtherArguments conversion : WITH_OTHER_ARGUMENTS) {
            for (List<DataType> union : conversion.unions()) {
                assertUnionResolves(
                    conversion.function(),
                    ", " + conversion.otherArguments(),
                    conversion.conversion(),
                    union.get(0),
                    union.get(1)
                );
            }
        }
    }

    /**
     * The conversion functions with optional arguments need them in {@link #WITH_OTHER_ARGUMENTS}, so that a new one is tested with
     * them, unless there is a reason why they can't be used on a union.
     */
    public void testConversionFunctionsWithOptionalArgumentsAreTestedWithThem() {
        Set<String> tested = new TreeSet<>();
        WITH_OTHER_ARGUMENTS.forEach(c -> tested.add(c.function()));
        Set<String> withOptionalArguments = new TreeSet<>();
        for (FunctionDefinition definition : TEST_FUNCTION_REGISTRY.listFunctions()) {
            if (ConvertFunction.class.isAssignableFrom(definition.clazz())
                && FoldablesConvertFunction.class.isAssignableFrom(definition.clazz()) == false
                && OptionalArgument.class.isAssignableFrom(definition.clazz())) {
                withOptionalArguments.add(definition.name());
            }
        }
        Set<String> expected = new TreeSet<>(tested);
        expected.addAll(OPTIONAL_ARGUMENTS_NOT_ON_UNIONS.keySet());
        assertThat(
            "conversion functions with optional arguments should be in WITH_OTHER_ARGUMENTS, or in OPTIONAL_ARGUMENTS_NOT_ON_UNIONS with a reason",
            withOptionalArguments,
            equalTo(expected)
        );
    }

    private void assertUnionResolves(
        String function,
        String otherArguments,
        Class<? extends Expression> expectedConversion,
        DataType first,
        DataType second
    ) {
        String failure = unionResolutionFailure(function, otherArguments, expectedConversion, first, second);
        assertNull(failure, failure);
    }

    /** Analyzes the function over a field mapped as the first type in one index and as the second type in another. */
    private LogicalPlan analyze(String function, String otherArguments, DataType first, DataType second) {
        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("foo", fieldResponseMap("value", first.esType())),
                fieldCapabilitiesIndexResponse("bar", fieldResponseMap("value", second.esType()))
            ),
            List.of()
        );
        return analyzer().addIndex(mergedResolution("foo,bar", caps))
            .query("FROM foo, bar | EVAL x = " + function + "(value" + otherArguments + ")");
    }

    /**
     * Why the function can't be used on the union of the two types, or {@code null} if it can. If given, every per-type conversion of
     * the union has to be the expected one.
     */
    private String unionResolutionFailure(
        String function,
        String otherArguments,
        @Nullable Class<? extends Expression> expectedConversion,
        DataType first,
        DataType second
    ) {
        String description = function + "(value" + otherArguments + ") over a union of [" + first + ", " + second + "]";
        LogicalPlan plan;
        try {
            plan = analyze(function, otherArguments, first, second);
        } catch (RuntimeException e) {
            return "failed to analyze " + description + ": " + e;
        }

        List<UnionTypeEsField> unionFields = new ArrayList<>();
        plan.forEachDown(EsRelation.class, relation -> {
            for (Attribute attribute : relation.output()) {
                if (attribute instanceof FieldAttribute field && field.field() instanceof UnionTypeEsField union) {
                    unionFields.add(union);
                }
            }
        });
        if (expectedConversion != null && unionFields.isEmpty()) {
            return description + ": expected a union field converted by [" + expectedConversion.getSimpleName() + "], found none";
        }
        // Some unions don't need a per-type conversion, as the types can be resolved on their own, like date and date_nanos
        for (UnionTypeEsField union : unionFields) {
            for (Expression conversion : union.getConversionExpressions()) {
                if (expectedConversion != null && expectedConversion.isInstance(conversion) == false) {
                    return description
                        + ": expected the conversion to be ["
                        + expectedConversion.getSimpleName()
                        + "], found: "
                        + conversion;
                }
                if (conversion instanceof OnlySurrogateExpression) {
                    return description + ": the conversion is a surrogate: " + conversion;
                }
                try {
                    SerializationTestUtils.assertSerialization(conversion);
                } catch (RuntimeException | AssertionError e) {
                    return description + ": the conversion [" + conversion + "] can't be serialized: " + e;
                }
            }
        }
        return null;
    }

    private static boolean isConversionOfFields(FunctionDefinition definition) {
        return ConvertFunction.class.isAssignableFrom(definition.clazz())
            && FoldablesConvertFunction.class.isAssignableFrom(definition.clazz()) == false
            && conversionOf(definition, DataType.KEYWORD) != null;
    }

    /**
     * The types a conversion function accepts and that can be in a union here, sorted by name. Found by analyzing the function over
     * a field of each type, not by asking the function, so a type dropped by mistake from what it supports is noticed.
     */
    private List<DataType> acceptedUnionableTypes(FunctionDefinition definition) {
        return UNIONABLE_TYPES.stream().filter(type -> {
            try {
                analyze(definition.name(), "", type, type);
                return true;
            } catch (RuntimeException e) {
                return false;
            }
        }).sorted(Comparator.comparing(DataType::typeName)).toList();
    }

    /** The function applied to just a field of the type, or {@code null} if it doesn't accept that. */
    private static ConvertFunction conversionOf(FunctionDefinition definition, DataType type) {
        Expression field = new Literal(Source.EMPTY, type == DataType.KEYWORD ? new BytesRef("x") : null, type);
        try {
            Object function = definition.build(new UnresolvedFunction(Source.EMPTY, definition.name(), List.of(field)), TEST_CFG);
            return function instanceof ConvertFunction convert ? convert : null;
        } catch (RuntimeException e) {
            return null;
        }
    }
}
