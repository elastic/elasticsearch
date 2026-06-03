/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PrometheusHistogramQuantile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests.randomSource;
import static org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests.randomChild;
import static org.elasticsearch.xpack.esql.plan.logical.EsRelationSerializationTests.randomEsRelation;

/**
 * Needed to override tests in {@link org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests} because
 * {@link HistogramQuantile} is not safe to construct via reflection alone:
 * <ul>
 * <li>The quantile argument is required — the constructor reads {@code parameters.getFirst()}, but
 * {@link org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests} typically supplies an empty parameter list.</li>
 * <li>The node uses {@link org.elasticsearch.xpack.esql.expression.function.aggregate.PrometheusHistogramQuantile#PROMQL_DEFINITION},
 * not the generic {@code VECTOR} stub that {@link org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests} picks for
 * {@link org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition} arguments.</li>
 * <li>{@link HistogramQuantile#output()} filters the {@value HistogramQuantile#LE_LABEL} label, so believable children
 * need classic-histogram bucket dimensions when they expose concrete labels.</li>
 * </ul>
 * Provides {@link #randomHistogramQuantile()} for {@link org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests} to delegate to.
 */
public class HistogramQuantileTests extends AbstractNodeTestCase<HistogramQuantile, LogicalPlan> {

    public static HistogramQuantile randomHistogramQuantile() {
        Source source = randomSource();
        return new HistogramQuantile(
            source,
            randomChildWithLeLabel(),
            PrometheusHistogramQuantile.PROMQL_DEFINITION,
            List.of(randomQuantile(source))
        );
    }

    private static Expression randomQuantile(Source source) {
        return Literal.fromDouble(source, randomDouble());
    }

    private static LogicalPlan randomChildWithLeLabel() {
        if (randomBoolean()) {
            return randomChild(0);
        }
        EsRelation relation = randomEsRelation();
        List<Attribute> output = new ArrayList<>(relation.output());
        Source source = relation.source();
        output.add(
            new FieldAttribute(
                source,
                null,
                null,
                "labels." + HistogramQuantile.LE_LABEL,
                new EsField("labels.le", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION)
            )
        );
        return new EsRelation(
            source,
            relation.indexPattern(),
            relation.indexMode(),
            relation.originalIndices(),
            relation.concreteIndices(),
            relation.indexNameWithModes(),
            output
        );
    }

    @Override
    protected HistogramQuantile randomInstance() {
        return randomHistogramQuantile();
    }

    @Override
    protected HistogramQuantile mutate(HistogramQuantile instance) {
        Supplier<HistogramQuantile> option = randomFrom(
            List.of(
                () -> new HistogramQuantile(
                    instance.source(),
                    randomValueOtherThan(instance.child(), () -> randomChildWithLeLabel()),
                    instance.definition(),
                    instance.parameters()
                ),
                () -> new HistogramQuantile(
                    instance.source(),
                    instance.child(),
                    instance.definition(),
                    List.of(randomValueOtherThan(instance.quantile(), () -> randomQuantile(instance.source())))
                )
            )
        );
        return option.get();
    }

    @Override
    protected HistogramQuantile copy(HistogramQuantile instance) {
        return new HistogramQuantile(instance.source(), instance.child(), instance.definition(), instance.parameters());
    }

    @Override
    public void testTransform() {
        HistogramQuantile node = randomHistogramQuantile();

        List<Expression> newParameters = List.of(randomValueOtherThan(node.quantile(), () -> randomQuantile(node.source())));
        assertEquals(
            new HistogramQuantile(node.source(), node.child(), node.definition(), newParameters),
            node.transformPropertiesOnly(Object.class, p -> Objects.equals(p, node.parameters()) ? newParameters : p)
        );

        PromqlFunctionDefinition newDefinition = randomValueOtherThan(node.definition(), () -> Rate.PROMQL_DEFINITION);
        assertEquals(
            new HistogramQuantile(node.source(), node.child(), newDefinition, node.parameters()),
            node.transformPropertiesOnly(Object.class, p -> Objects.equals(p, node.definition()) ? newDefinition : p)
        );
    }

    @Override
    public void testReplaceChildren() {
        HistogramQuantile node = randomHistogramQuantile();
        LogicalPlan newChild = randomValueOtherThan(node.child(), () -> randomChildWithLeLabel());
        assertEquals(new HistogramQuantile(node.source(), newChild, node.definition(), node.parameters()), node.replaceChild(newChild));
    }

    public void testOutputPreservesTimeseriesInsteadOfRelationDimensions() {
        Source source = randomSource();
        EsRelation relation = randomEsRelationWithLeAndIrrelevantLabel(source);
        LogicalPlan child = new WithinSeriesAggregate(source, relation, Rate.PROMQL_DEFINITION, List.of());

        HistogramQuantile node = new HistogramQuantile(
            source,
            child,
            PrometheusHistogramQuantile.PROMQL_DEFINITION,
            List.of(randomQuantile(source))
        );

        assertEquals(List.of(MetadataAttribute.TIMESERIES), node.output().stream().map(Attribute::name).toList());
    }

    private static EsRelation randomEsRelationWithLeAndIrrelevantLabel(Source source) {
        EsRelation relation = randomEsRelation();
        List<Attribute> output = new ArrayList<>(relation.output());
        output.add(dimension(source, "labels." + HistogramQuantile.LE_LABEL));
        output.add(dimension(source, "labels.irrelevant"));
        return new EsRelation(
            source,
            relation.indexPattern(),
            relation.indexMode(),
            relation.originalIndices(),
            relation.concreteIndices(),
            relation.indexNameWithModes(),
            output
        );
    }

    private static FieldAttribute dimension(Source source, String name) {
        return new FieldAttribute(
            source,
            null,
            null,
            name,
            new EsField(name, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION)
        );
    }
}
