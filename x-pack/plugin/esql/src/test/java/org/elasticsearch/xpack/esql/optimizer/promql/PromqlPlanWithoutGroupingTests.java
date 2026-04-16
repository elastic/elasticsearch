/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.TestLocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslatePromqlToEsqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class PromqlPlanWithoutGroupingTests extends AbstractPromqlPlanOptimizerTests {

    @Before
    public void assumePromqlWithoutGroupingEnabled() {
        assumeTrue("Requires PROMQL WITHOUT capability", EsqlCapabilities.Cap.PROMQL_WITHOUT_GROUPING.isEnabled());
    }

    public void testWithoutGroupingProducesTimeSeriesOutput() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(sum without (pod) (network.bytes_in))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", MetadataAttribute.TIMESERIES)));

        var tsAggregates = plan.collect(TimeSeriesAggregate.class);
        assertThat(tsAggregates, hasSize(1));

        var aggregates = plan.collect(Aggregate.class);
        assertThat(aggregates, not(empty()));

        var timeSeriesMetadata = plan.collect(EsRelation.class)
            .stream()
            .flatMap(relation -> relation.output().stream())
            .filter(TimeSeriesMetadataAttribute.class::isInstance)
            .map(TimeSeriesMetadataAttribute.class::cast)
            .findFirst()
            .orElse(null);
        assertNotNull(timeSeriesMetadata);
        assertThat(timeSeriesMetadata.withoutFields(), hasItem("pod"));
    }

    public void testWithoutGroupingSurvivesDataNodePlanSerialization() {
        String query = "PROMQL index=k8s step=1h result=(sum without (pod) (network.bytes_in))";
        var logical = logicalOptimizerWithLatestVersion.optimize(planPromql(query, false));
        var physical = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(EsqlTestUtils.TEST_CFG, TransportVersion.current())).optimize(
            new Mapper().map(new Versioned<>(logical, TransportVersion.current()))
        );
        PhysicalPlan dataNodePlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(physical, EsqlTestUtils.TEST_CFG).v2();
        LogicalPlan fragment = as(as(dataNodePlan, ExchangeSinkExec.class).child(), FragmentExec.class).fragment();
        var fragmentPackedTimeSeriesValues = fragment.collect(TimeSeriesAggregate.class)
            .stream()
            .flatMap(aggregate -> aggregate.aggregates().stream())
            .flatMap(aggregate -> aggregate.collect(DimensionValues.class).stream())
            .map(DimensionValues::field)
            .filter(Attribute.class::isInstance)
            .map(Attribute.class::cast)
            .filter(attr -> MetadataAttribute.TIMESERIES.equals(attr.name()))
            .toList();
        assertThat(fragment.toString(), fragmentPackedTimeSeriesValues, hasSize(1));
        assertThat(as(fragmentPackedTimeSeriesValues.getFirst(), TimeSeriesMetadataAttribute.class).withoutFields(), hasItem("pod"));
        PhysicalPlan deserializedDataNodePlan = SerializationTestUtils.serializeDeserialize(
            dataNodePlan,
            StreamOutput::writeNamedWriteable,
            in -> in.readNamedWriteable(PhysicalPlan.class),
            EsqlTestUtils.TEST_CFG
        );
        LogicalPlan deserializedFragment = as(as(deserializedDataNodePlan, ExchangeSinkExec.class).child(), FragmentExec.class).fragment();
        var deserializedPackedTimeSeriesValues = deserializedFragment.collect(TimeSeriesAggregate.class)
            .stream()
            .flatMap(aggregate -> aggregate.aggregates().stream())
            .flatMap(aggregate -> aggregate.collect(DimensionValues.class).stream())
            .map(DimensionValues::field)
            .filter(Attribute.class::isInstance)
            .map(Attribute.class::cast)
            .filter(attr -> MetadataAttribute.TIMESERIES.equals(attr.name()))
            .toList();
        assertThat(deserializedFragment.toString(), deserializedPackedTimeSeriesValues, hasSize(1));
        assertThat(as(deserializedPackedTimeSeriesValues.getFirst(), TimeSeriesMetadataAttribute.class).withoutFields(), hasItem("pod"));

        var localLogical = new LocalLogicalPlanOptimizer(
            new LocalLogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), SearchStats.EMPTY)
        );
        LogicalPlan localizedFragment = localLogical.localOptimize(deserializedFragment);
        var localizedPackedTimeSeriesValues = localizedFragment.collect(TimeSeriesAggregate.class)
            .stream()
            .flatMap(aggregate -> aggregate.aggregates().stream())
            .flatMap(aggregate -> aggregate.collect(DimensionValues.class).stream())
            .map(DimensionValues::field)
            .filter(Attribute.class::isInstance)
            .map(Attribute.class::cast)
            .filter(attr -> MetadataAttribute.TIMESERIES.equals(attr.name()))
            .toList();
        assertThat(localizedFragment.toString(), localizedPackedTimeSeriesValues, hasSize(1));
        assertThat(as(localizedPackedTimeSeriesValues.getFirst(), TimeSeriesMetadataAttribute.class).withoutFields(), hasItem("pod"));

        PhysicalPlan mappedLocalizedFragment = LocalMapper.INSTANCE.map(localizedFragment);
        var mappedPackedTimeSeriesValues = mappedLocalizedFragment.collect(
            org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec.class
        )
            .stream()
            .flatMap(aggregate -> aggregate.aggregates().stream())
            .flatMap(aggregate -> aggregate.collect(DimensionValues.class).stream())
            .map(DimensionValues::field)
            .filter(Attribute.class::isInstance)
            .map(Attribute.class::cast)
            .filter(attr -> MetadataAttribute.TIMESERIES.equals(attr.name()))
            .toList();
        assertThat(mappedLocalizedFragment.toString(), mappedPackedTimeSeriesValues, hasSize(1));
        assertThat(as(mappedPackedTimeSeriesValues.getFirst(), TimeSeriesMetadataAttribute.class).withoutFields(), hasItem("pod"));

        var localPhysical = new TestLocalPhysicalPlanOptimizer(
            new LocalPhysicalOptimizerContext(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(true),
                EsqlTestUtils.TEST_CFG,
                FoldContext.small(),
                SearchStats.EMPTY
            ),
            true
        );
        var localizedDataNodePlan = PlannerUtils.localPlan(deserializedDataNodePlan, localLogical, localPhysical, null);

        FieldExtractExec readTimeSeries = localizedDataNodePlan.collect(FieldExtractExec.class)
            .stream()
            .filter(fieldExtract -> Expressions.names(fieldExtract.attributesToExtract()).contains(MetadataAttribute.TIMESERIES))
            .findFirst()
            .orElse(null);
        assertNotNull(localizedDataNodePlan.toString(), readTimeSeries);

        FieldAttribute extractedTimeSeries = as(readTimeSeries.attributesToExtract().getFirst(), FieldAttribute.class);
        assertThat(extractedTimeSeries.fieldName().string(), equalTo(SourceFieldMapper.NAME));
        FunctionEsField extractedField = as(extractedTimeSeries.field(), FunctionEsField.class);
        var functionConfig = as(extractedField.functionConfig(), BlockLoaderFunctionConfig.TimeSeriesMetadata.class);
        assertThat(functionConfig.withoutFields(), hasItem("pod"));
    }

    public void testNestedWithoutOverByProducesTimeSeriesOutput() {
        var plan = planPromql("PROMQL index=k8s step=1h result=(sum without (pod) (sum by (cluster, region, pod) (network.cost)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", MetadataAttribute.TIMESERIES)));
    }

    public void testNestedWithoutOverByInnerByHasTimeSeriesWithout() {
        LogicalPlan analyzed = planPromql(
            "PROMQL index=k8s step=1h result=(sum without (pod) (sum by (cluster, region, pod) (network.cost)))",
            false
        );
        PromqlCommand promql = analyzed.collect(PromqlCommand.class).getFirst();

        LogicalPlan translated = new TranslatePromqlToEsqlPlan().apply(promql, logicalOptimizerCtx);
        TimeSeriesAggregate innerAggregate = translated.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(
            innerAggregate.toString(),
            innerAggregate.groupings().stream().filter(grouping -> Alias.unwrap(grouping) instanceof TimeSeriesWithout).toList(),
            hasSize(1)
        );
    }

    public void testParentBySynthesizesConsumedBindingFromWithoutCarrier() {
        LogicalPlan analyzed = planPromql("PROMQL index=k8s step=1h result=(sum by (cluster) (sum without (pod) (network.cost)))", false);
        PromqlCommand promql = analyzed.collect(PromqlCommand.class).getFirst();

        LogicalPlan translated = new TranslatePromqlToEsqlPlan().apply(promql, logicalOptimizerCtx);
        TimeSeriesAggregate innerAggregate = translated.collect(TimeSeriesAggregate.class).getFirst();
        assertTrue(innerAggregate.groupings().stream().anyMatch(grouping -> Alias.unwrap(grouping) instanceof TimeSeriesWithout));
        assertThat(innerAggregate.aggregates().stream().map(NamedExpression::name).toList(), hasItem("cluster"));
    }

    public void testNestedWithoutOverWithoutIsRejectedByVerifier() {
        VerificationException e = assertThrows(
            VerificationException.class,
            () -> planPromql("PROMQL index=k8s step=1h result=(sum without (region) (sum without (pod) (network.cost)))")
        );
        assertThat(e.getMessage(), containsString("nested WITHOUT over WITHOUT is not supported at this time"));
    }

    public void testNestedWithoutOverPartialByProducesTimeSeriesOutput() {
        var plan = planPromql("PROMQL index=k8s step=1h result=(sum without (pod) (sum by (cluster, pod) (network.cost)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", MetadataAttribute.TIMESERIES)));
    }

    public void testAvgOverWithoutRangeAggregationPlans() {
        planPromql("PROMQL index=k8s step=1h result=(avg(sum without (pod, region) (avg_over_time(network.cost[1h]))))");
    }

    public void testSumByRegionOverSumWithoutRegionProducesScalar() {
        var plan = planPromql("PROMQL index=k8s step=1h result=(sum by (region) (sum without (region) (network.cost)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem("result"));
        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem("step"));
    }

    public void testWithoutOverByOverWithoutIsRejectedByVerifier() {
        VerificationException e = assertThrows(
            VerificationException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1h result=(sum without (region) (sum by (cluster, region) (sum without (pod) (network.cost))))"
            )
        );
        assertThat(e.getMessage(), containsString("nested WITHOUT over WITHOUT is not supported at this time"));
    }

    public void testByOverWithoutOverByProducesExactOutput() {
        var plan = planPromql(
            "PROMQL index=k8s step=1h result=(sum by (cluster) (sum without (pod) (sum by (cluster, region, pod) (network.cost))))"
        );
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", "cluster")));
    }

    public void testScalarOverWithoutProducesScalarOutput() {
        var plan = planPromql("PROMQL index=k8s step=1h result=(scalar(sum without (pod, region) (avg_over_time(network.cost[1h]))))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
    }

    public void testWithoutAbsentLabelIsNoOp() {
        LogicalPlan analyzed = planPromql(
            "PROMQL index=k8s step=1h result=(sum by (cluster) (sum without (does_not_exist) (network.cost)))",
            false
        );
        PromqlCommand promql = analyzed.collect(PromqlCommand.class).getFirst();
        LogicalPlan translated = new TranslatePromqlToEsqlPlan().apply(promql, logicalOptimizerCtx);
        TimeSeriesAggregate innerTsa = translated.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(innerTsa.aggregates().stream().map(NamedExpression::name).toList(), hasItem("cluster"));
    }

    public void testWithoutAllKnownLabelsProducesEmptyGrouping() {
        var plan = planPromql("PROMQL index=k8s step=1h result=(sum without (cluster, region, pod) (network.cost))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", MetadataAttribute.TIMESERIES)));
    }

    public void testWithoutPostfixSyntaxPlans() {
        planPromql("PROMQL index=k8s step=1h result=(sum(avg_over_time(network.cost[1h])) without (pod, region))");
    }

    public void testWithoutTrailingCommaPlans() {
        planPromql("PROMQL index=k8s step=1h result=(sum without (pod, region,) (avg_over_time(network.cost[1h])))");
    }

    public void testWithoutEmptyLabelListPlans() {
        LogicalPlan analyzed = planPromql(
            "PROMQL index=k8s step=1h result=(sum by (cluster) (sum without () (avg_over_time(network.cost[1h]))))",
            false
        );
        PromqlCommand promql = analyzed.collect(PromqlCommand.class).getFirst();
        LogicalPlan translated = new TranslatePromqlToEsqlPlan().apply(promql, logicalOptimizerCtx);
        TimeSeriesAggregate innerTsa = translated.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(innerTsa.aggregates().stream().map(NamedExpression::name).toList(), hasItem("cluster"));
    }

    public void testScalarOverMaxOfWithoutProducesScalarOutput() {
        var plan = planPromql("PROMQL index=k8s step=1h result=(scalar(max(sum without (pod, region) (avg_over_time(network.cost[1h])))))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
    }
}
