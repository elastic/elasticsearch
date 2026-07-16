/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@FunctionName("without")
public class TimeSeriesWithoutTests extends AbstractScalarFunctionTestCase {
    public TimeSeriesWithoutTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(
            new TestCaseSupplier(
                "keyword dimension",
                List.of(DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(new BytesRef("pod"), DataType.KEYWORD, "dimension")),
                    "",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(""))
                ).withoutEvaluator()
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesWithout(source, args);
    }

    @Override
    public void testFold() {
        // WITHOUT requires time-series dimension field attributes, so it can't be resolved or folded from literals.
    }

    public void testResolveTypeAcceptsDimensionFields() {
        TimeSeriesWithout without = new TimeSeriesWithout(Source.EMPTY, List.of(dimension("pod"), dimension("host")));
        assertThat(without.resolveType().resolved(), equalTo(true));
    }

    public void testResolveTypeAcceptsNoArguments() {
        TimeSeriesWithout without = new TimeSeriesWithout(Source.EMPTY, List.of());
        assertThat(without.resolveType().resolved(), equalTo(true));
    }

    public void testResolveTypeAcceptsNonDimensionField() {
        // #149793: excluded labels are matched by name, so a non-dimension field is accepted - excluding a label that
        // is not a dimension (or not present at all) is simply a no-op, not an error.
        TimeSeriesWithout without = new TimeSeriesWithout(Source.EMPTY, List.of(field("not_a_dimension", DataType.KEYWORD)));
        assertThat(without.resolveType().resolved(), equalTo(true));
    }

    public void testResolveTypeRejectsNonFieldExpression() {
        TimeSeriesWithout without = new TimeSeriesWithout(Source.EMPTY, List.of(new Literal(Source.EMPTY, 1, DataType.INTEGER)));
        Expression.TypeResolution resolution = without.resolveType();
        assertThat(resolution.unresolved(), equalTo(true));
        assertThat(resolution.message(), containsString("requires label names"));
        assertThat(resolution.message(), containsString("integer"));
    }

    public void testExcludedFieldNames() {
        TimeSeriesWithout without = new TimeSeriesWithout(Source.EMPTY, List.of(dimension("pod"), dimension("host")));
        assertThat(excludedNames(without), equalTo(Set.of("pod", "host")));
    }

    public void testExcludedFieldNamesIsEmptyForNoArguments() {
        assertThat(excludedNames(new TimeSeriesWithout(Source.EMPTY, List.of())), equalTo(Set.<String>of()));
    }

    public void testExcludedFieldNamesDeduplicatesAndPreservesOrder() {
        TimeSeriesWithout without = new TimeSeriesWithout(Source.EMPTY, List.of(dimension("pod"), dimension("host"), dimension("pod")));
        assertThat(new ArrayList<>(excludedNames(without)), contains("pod", "host"));
    }

    public void testDataTypeIsKeyword() {
        assertThat(new TimeSeriesWithout(Source.EMPTY, List.of()).dataType(), equalTo(DataType.KEYWORD));
    }

    public void testIsNotNullable() {
        assertThat(new TimeSeriesWithout(Source.EMPTY, List.of()).nullable(), equalTo(Nullability.FALSE));
    }

    public void testCreateNamedExpressionWrapsTimeseriesMetadataColumn() {
        TimeSeriesWithout without = new TimeSeriesWithout(Source.EMPTY, List.of(dimension("pod")));
        Alias attribute = (Alias) without.createNamedExpression();
        assertThat(attribute.name(), equalTo(MetadataAttribute.TIMESERIES));
        assertSame(without, attribute.child());
    }

    public void testReplaceChildren() {
        TimeSeriesWithout without = new TimeSeriesWithout(Source.EMPTY, List.of(dimension("pod")));
        TimeSeriesWithout replaced = without.replaceChildren(List.of(dimension("host")));
        assertThat(replaced.children(), hasSize(1));
        assertThat(excludedNames(replaced), equalTo(Set.of("host")));
    }

    /** The PromQL label keys a {@code WITHOUT} excludes, derived from its children (deduplicated, order-preserving). */
    private static Set<String> excludedNames(TimeSeriesWithout without) {
        Set<String> names = new LinkedHashSet<>();
        for (Expression child : without.children()) {
            names.add(((FieldAttribute) child).fieldName().string());
        }
        return names;
    }

    /**
     * Builds a time-series dimension {@link FieldAttribute}, the only kind of argument {@code WITHOUT} accepts.
     */
    private static FieldAttribute dimension(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            name,
            new EsField(name, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION)
        );
    }
}
