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
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
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
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
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
        assertThat(timeSeriesMetadata.excludedFields(), hasItem("pod"));
    }

    /**
     * In PromQL {@code without ()} (empty parens) means "group by every label", i.e. the full runtime label set - not
     * "no grouping". The innermost aggregate must therefore still own a {@code _timeseries} grouping key (with an empty
     * exclusion set), otherwise the final PromQL projection references a {@code _timeseries} the plan never produced.
     */
    public void testTopLevelWithoutEmptyProducesTimeSeriesOutput() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(sum without () (network.bytes_in))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", MetadataAttribute.TIMESERIES)));

        var timeSeriesMetadata = plan.collect(EsRelation.class)
            .stream()
            .flatMap(relation -> relation.output().stream())
            .filter(TimeSeriesMetadataAttribute.class::isInstance)
            .map(TimeSeriesMetadataAttribute.class::cast)
            .findFirst()
            .orElse(null);
        assertNotNull(timeSeriesMetadata);
        assertThat(timeSeriesMetadata.excludedFields(), empty());
    }

    /**
     * Negative pin: a bare aggregate with no grouping clause ({@code sum(...)}) collapses every series into one result,
     * so it must NOT carry a {@code _timeseries} grouping. Guards against the {@code without ()} fix over-triggering.
     */
    public void testBareAggregateDoesNotProduceTimeSeriesOutput() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(sum (network.bytes_in))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), not(hasItem(MetadataAttribute.TIMESERIES)));
    }

    /**
     * Negative pin: {@code by ()} (empty parens) groups by nothing - the dual of {@code without ()} - and must NOT carry
     * a {@code _timeseries} grouping either.
     */
    public void testByEmptyDoesNotProduceTimeSeriesOutput() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(sum by () (network.bytes_in))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), not(hasItem(MetadataAttribute.TIMESERIES)));
    }

    /**
     * Regression for #149793: {@code without(label)} must exclude the label by name even when it does not resolve to a
     * dimension {@link org.elasticsearch.xpack.esql.core.expression.FieldAttribute}. Before the fix, the dimension-only
     * filter silently dropped such exclusions, so labels like Prometheus {@code instance} were retained in the
     * {@code _timeseries} output. Here {@code event} is a mapped non-dimension field, exercising that path.
     */
    public void testWithoutExcludesNonDimensionLabelByName() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(sum without (pod, event) (network.bytes_in))", false)
        );

        var timeSeriesMetadata = plan.collect(EsRelation.class)
            .stream()
            .flatMap(relation -> relation.output().stream())
            .filter(TimeSeriesMetadataAttribute.class::isInstance)
            .map(TimeSeriesMetadataAttribute.class::cast)
            .findFirst()
            .orElse(null);
        assertNotNull(timeSeriesMetadata);
        assertThat(timeSeriesMetadata.excludedFields(), hasItems("pod", "event"));
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
        assertThat(as(fragmentPackedTimeSeriesValues.getFirst(), TimeSeriesMetadataAttribute.class).excludedFields(), hasItem("pod"));
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
        assertThat(as(deserializedPackedTimeSeriesValues.getFirst(), TimeSeriesMetadataAttribute.class).excludedFields(), hasItem("pod"));

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
        assertThat(as(localizedPackedTimeSeriesValues.getFirst(), TimeSeriesMetadataAttribute.class).excludedFields(), hasItem("pod"));

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
        assertThat(as(mappedPackedTimeSeriesValues.getFirst(), TimeSeriesMetadataAttribute.class).excludedFields(), hasItem("pod"));

        var localPhysical = new LocalPhysicalPlanOptimizer(
            new LocalPhysicalOptimizerContext(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(true),
                EsqlTestUtils.TEST_CFG,
                FoldContext.small(),
                SearchStats.EMPTY
            )
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
        assertThat(functionConfig.skipFieldNames(), hasItem("pod"));
    }

    public void testNestedWithoutOverByProducesConcreteOutput() {
        // `without(pod)` over `by(cluster,region,pod)` drops pod from the concrete BY keys: the result is grouped by
        // {cluster, region}, not the opaque `_timeseries` identity (which would leak every other label).
        var plan = planPromql("PROMQL index=k8s step=1h result=(sum without (pod) (sum by (cluster, region, pod) (network.cost)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", "cluster", "region")));
    }

    public void testNestedWithoutOverByHasNoTimeSeriesGrouping() {
        // The inner BY enumerates concrete labels, so the outer WITHOUT is a concrete re-grouping over those keys - no
        // `_timeseries` identity is needed (or produced) anywhere in the plan.
        LogicalPlan analyzed = planPromql(
            "PROMQL index=k8s step=1h result=(sum without (pod) (sum by (cluster, region, pod) (network.cost)))",
            false
        );
        EsRelation esRelation = analyzed.collect(EsRelation.class).getFirst();
        var tsmaList = esRelation.expressions().stream().filter(field -> field instanceof TimeSeriesMetadataAttribute).toList();
        assertThat(tsmaList, hasSize(0));
    }

    public void testParentBySynthesizesConsumedBindingFromWithoutCarrier() {
        LogicalPlan analyzed = planPromql("PROMQL index=k8s step=1h result=(sum by (cluster) (sum without (pod) (network.cost)))", false);
        EsRelation esRelation = analyzed.collect(EsRelation.class).getFirst();
        var tsmaList = esRelation.expressions().stream().filter(field -> field instanceof TimeSeriesMetadataAttribute).toList();
        assertThat(tsmaList, hasSize(1));
        assertEquals(((TimeSeriesMetadataAttribute) tsmaList.getFirst()).excludedFields(), Set.of("pod"));
        TimeSeriesAggregate innerAggregate = analyzed.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(
            innerAggregate.aggregates()
                .stream()
                .filter(
                    aggregate -> Alias.unwrap(aggregate) instanceof DimensionValues values
                        && values.field() instanceof FieldAttribute field
                        && field.name().equals("cluster")
                )
                .toList(),
            hasSize(1)
        );
    }

    public void testNestedWithoutOverWithoutIsRejectedByVerifier() {
        VerificationException e = assertThrows(
            VerificationException.class,
            () -> planPromql("PROMQL index=k8s step=1h result=(sum without (region) (sum without (pod) (network.cost)))")
        );
        assertThat(e.getMessage(), containsString("nested WITHOUT over WITHOUT is not supported at this time"));
    }

    public void testNestedWithoutOverPartialByProducesConcreteOutput() {
        // `without(pod)` over `by(cluster,pod)` drops pod, leaving the concrete {cluster}.
        var plan = planPromql("PROMQL index=k8s step=1h result=(sum without (pod) (sum by (cluster, pod) (network.cost)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", "cluster")));
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

    public void testWithoutAbsentLabelRetainsFullLabelSet() {
        // `without(<absent label>)` excludes a label that is not present in the series - a no-op exclusion, equivalent
        // to `without ()`. It therefore groups by the full runtime label set, so the inner aggregate MUST own a
        // `_timeseries` grouping (with no real exclusion); it is not collapsed away. (Pre-#149793 this wrongly dropped
        // the unmapped label and produced no `_timeseries`.)
        LogicalPlan analyzed = planPromql(
            "PROMQL index=k8s step=1h result=(sum by (cluster) (sum without (does_not_exist) (network.cost)))",
            false
        );
        EsRelation esRelation = analyzed.collect(EsRelation.class).getFirst();
        var tsmaList = esRelation.expressions().stream().filter(field -> field instanceof TimeSeriesMetadataAttribute).toList();
        assertThat(tsmaList, hasSize(1));
    }

    public void testWithoutAllKnownLabelsProducesEmptyGrouping() {
        var plan = planPromql("PROMQL index=k8s step=1h result=(sum without (cluster, region, pod) (network.cost))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", MetadataAttribute.TIMESERIES)));
    }

    public void testWithoutPostfixSyntaxPlans() {
        planPromql("PROMQL index=k8s step=1h result=(sum(avg_over_time(network.cost[1h])) without (pod, region))");
    }

    /**
     * Regression test for OTel passthrough alias exclusion: {@code without(cpu)} on an OTel TSDB index must
     * resolve the short label name {@code cpu} to the concrete passthrough dimension {@code attributes.cpu},
     * so the {@code _timeseries} block loader receives the concrete field name and correctly excludes it.
     * Before the fix, the plan passed {@code "cpu"} in {@code excludedFields()} but the block loader compared
     * against {@code "attributes.cpu"} (the concrete field name) and the exclusion never matched.
     */
    public void testWithoutOtelAttributeShortNameExcludesConcretePassthroughDimension() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=otel-metrics step=1h result=(sum without (cpu) (metrics.system.cpu.time))", false)
        );

        var timeSeriesMetadata = plan.collect(EsRelation.class)
            .stream()
            .flatMap(relation -> relation.output().stream())
            .filter(TimeSeriesMetadataAttribute.class::isInstance)
            .map(TimeSeriesMetadataAttribute.class::cast)
            .findFirst()
            .orElse(null);
        assertNotNull(timeSeriesMetadata);
        assertThat(timeSeriesMetadata.excludedFields(), hasItem("cpu"));
    }

    /**
     * Regression test for OTel resource-attributes passthrough alias exclusion. The plan-level fix does not
     * strip {@code resource.attributes.} prefix, so {@code excludedFields()} contains {@code "host.name"} (the
     * short root-level alias). The block loader fix resolves the alias to the concrete dimension
     * {@code resource.attributes.host.name} via {@code MappingLookup.getFieldType}, ensuring the exclusion
     * actually matches the dimension the loader enumerates.
     */
    public void testWithoutResourceAttributeShortNameExcludesConcretePassthroughDimension() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=otel-metrics step=1h result=(sum without (host.name) (metrics.system.cpu.time))", false)
        );

        var timeSeriesMetadata = plan.collect(EsRelation.class)
            .stream()
            .flatMap(relation -> relation.output().stream())
            .filter(TimeSeriesMetadataAttribute.class::isInstance)
            .map(TimeSeriesMetadataAttribute.class::cast)
            .findFirst()
            .orElse(null);
        assertNotNull(timeSeriesMetadata);
        assertThat(timeSeriesMetadata.excludedFields(), hasItem("host.name"));
    }

    public void testWithoutTrailingCommaPlans() {
        planPromql("PROMQL index=k8s step=1h result=(sum without (pod, region,) (avg_over_time(network.cost[1h])))");
    }

    public void testWithoutEmptyLabelListPlans() {
        LogicalPlan analyzed = planPromql(
            "PROMQL index=k8s step=1h result=(sum by (cluster) (sum without () (avg_over_time(network.cost[1h]))))",
            false
        );
        TimeSeriesAggregate innerAggregate = analyzed.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(
            innerAggregate.aggregates()
                .stream()
                .filter(
                    aggregate -> Alias.unwrap(aggregate) instanceof DimensionValues values
                        && values.field() instanceof FieldAttribute field
                        && field.name().equals("cluster")
                )
                .toList(),
            hasSize(1)
        );
        // TimeSeriesMetadataAttribute shouldn't be getting created if without has no label
        EsRelation esRelation = analyzed.collect(EsRelation.class).getFirst();
        var tsmaList = esRelation.expressions().stream().filter(field -> field instanceof TimeSeriesMetadataAttribute).toList();
        assertThat(tsmaList, hasSize(0));
    }

    public void testScalarOverMaxOfWithoutProducesScalarOutput() {
        var plan = planPromql("PROMQL index=k8s step=1h result=(scalar(max(sum without (pod, region) (avg_over_time(network.cost[1h])))))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
    }
}
