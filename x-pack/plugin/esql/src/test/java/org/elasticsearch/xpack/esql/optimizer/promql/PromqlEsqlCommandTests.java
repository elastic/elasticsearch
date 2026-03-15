/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.TestLocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslatePromqlToEsqlPlan;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
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

import java.time.Duration;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class PromqlEsqlCommandTests extends AbstractPromqlPlanOptimizerTests {

    public void testPromqlTrailingSpaces() {
        planPromql("PROMQL index=k8s step=1h (max(network.bytes_in)) ");
        planPromql("PROMQL index=k8s step=1h (max(network.bytes_in)) | SORT step");
    }

    public void testPromqlMaxOfLongField() {
        var plan = planPromql("PROMQL index=k8s step=1h max(network.bytes_in)");
        // In PromQL, the output is always double
        assertThat(plan.output().getFirst().dataType(), equalTo(DataType.DOUBLE));
        assertThat(plan.output().getFirst().name(), equalTo("max(network.bytes_in)"));
    }

    public void testPromqlExplicitOutputName() {
        var plan = planPromql("PROMQL index=k8s step=1h max_bytes=(max(network.bytes_in))");
        assertThat(plan.output().getFirst().name(), equalTo("max_bytes"));
    }

    public void testSort() {
        var plan = planPromql("""
            PROMQL index=k8s step=1h (
                avg(network.bytes_in) by (pod)
              )
            | SORT step, pod, `avg(network.bytes_in) by (pod)`
            """);
        List<String> order = plan.collect(TopN.class)
            .getFirst()
            .order()
            .stream()
            .map(o -> as(o.child(), NamedExpression.class).name())
            .toList();
        assertThat(order, hasSize(3));
        assertThat(order, equalTo(List.of("step", "pod", "avg(network.bytes_in) by (pod)")));
    }

    public void testNonExistentFieldsOptimizesToEmptyPlan() {
        List.of("non_existent_metric", "network.eth0.rx{non_existent_label=\"value\"}", "avg(non_existent_metric)"
        // TODO because we wrap group-by-all aggregates into Values, this does not optimize away yet
        // "rate(non_existent_metric[5m])"
        ).forEach(query -> {
            var plan = planPromql("PROMQL index=k8s step=1m " + query);
            assertThat(as(plan, LocalRelation.class).supplier(), equalTo(EmptyLocalSupplier.EMPTY));
        });
    }

    public void testGroupByNonExistentLabel() {
        var plan = planPromql("PROMQL index=k8s step=1m result=(sum by (non_existent_label) (network.eth0.rx))");
        // equivalent to avg(network.eth0.rx) since the label does not exist
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
        // the non-existent label should not appear in the groupings
        plan.collect(Aggregate.class)
            .forEach(
                agg -> assertThat(
                    agg.groupings().stream().map(Attribute.class::cast).map(Attribute::name).toList(),
                    not(hasItem("non_existent_label"))
                )
            );
    }

    public void testAvgAvgOverTimeOutput() {
        var plan = planPromql("""
            PROMQL index=k8s step=1h ( avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h])) )
            | LIMIT 1000
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(3));

        var aggregate = plan.collect(Aggregate.class).getFirst();
        assertThat(aggregate.groupings(), hasSize(2));

        var evalMiddle = as(aggregate.child(), Eval.class);

        var tsAggregate = as(evalMiddle.child(), TimeSeriesAggregate.class);
        assertThat(tsAggregate.groupings(), hasSize(2));

        // verify TBUCKET duration plus reuse
        var evalBucket = as(tsAggregate.child(), Eval.class);
        assertThat(evalBucket.fields(), hasSize(1));
        var bucketAlias = as(evalBucket.fields().get(0), Alias.class);
        var bucket = as(bucketAlias.child(), Bucket.class);

        var bucketSpan = bucket.buckets();
        assertThat(bucketSpan.fold(FoldContext.small()), equalTo(Duration.ofHours(1)));

        var tbucketId = bucketAlias.toAttribute().id();
        assertThat(Expressions.attribute(tsAggregate.groupings().get(1)).id(), equalTo(tbucketId));
        assertThat(Expressions.attribute(aggregate.groupings().get(0)).id(), equalTo(tbucketId));
        assertThat(Expressions.attribute(project.projections().get(1)).id(), equalTo(tbucketId));

        // Filter should contain: IN(host-0, host-1, host-2, pod)
        var filter = as(evalBucket.child(), Filter.class);
        var in = as(filter.condition(), org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In.class);
        assertThat(in.list(), hasSize(3));

        as(filter.child(), EsRelation.class);
    }

    public void testImplicitRangeSelectorUsesStepWindow() {
        var plan = planPromql("""
            PROMQL index=k8s step=5m rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(5)));
    }

    public void testImplicitRangeSelectorUsesScrapeIntervalWhenStepIsSmaller() {
        var plan = planPromql("""
            PROMQL index=k8s step=15s rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testImplicitRangeSelectorRoundsWindowToStepMultiple() {
        var plan = planPromql("""
            PROMQL index=k8s step=20s scrape_interval=1m rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testImplicitRangeSelectorUsesInferredStepFromDefaultBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));

        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testImplicitRangeSelectorUsesInferredStepFromBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" buckets=6 rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));

        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));
    }

    public void testStartEndStep() {
        String testQuery = """
            PROMQL index=k8s start=$now-1h end=$now step=5m (
                avg(avg_over_time(network.bytes_in[5m]))
                )
            """;

        var plan = planPromql(testQuery);
        var filters = plan.collect(Filter.class);
        assertThat(
            filters.stream()
                .map(Filter::condition)
                .flatMap(c -> c.collect(FieldAttribute.class).stream())
                .map(FieldAttribute::name)
                .filter("@timestamp"::equals)
                .count(),
            equalTo(2L)
        );
    }

    public void testInferredStepUsesDefaultBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" (
                avg(avg_over_time(network.bytes_in[6m]))
              )
            """);
        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testInferredStepMinStepIsUnknownParameter() {
        ParsingException e = assertThrows(ParsingException.class, () -> planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" min_step=1s (
                avg(avg_over_time(network.bytes_in[6m]))
              )
            """));
        assertThat(e.getMessage(), containsString("Unknown parameter [min_step]"));
    }

    public void testInferredStepUsesBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" buckets=6 (
                avg(avg_over_time(network.bytes_in[1h]))
              )
            """);
        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));
    }

    private static void assumePromqlWithoutGrouping() {
        assumeTrue("Requires PROMQL WITHOUT capability", EsqlCapabilities.Cap.PROMQL_WITHOUT_GROUPING.isEnabled());
    }

    public void testWithoutGroupingProducesTimeSeriesOutput() {
        assumePromqlWithoutGrouping();
        var plan = logicalOptimizerWithLatestVersion.optimize(
            tsAnalyzer.analyze(TEST_PARSER.parseQuery("PROMQL index=k8s step=1h result=(sum without (pod) (network.bytes_in))"))
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
        assumePromqlWithoutGrouping();
        String query = "PROMQL index=k8s step=1h result=(sum without (pod) (network.bytes_in))";
        var logical = logicalOptimizerWithLatestVersion.optimize(tsAnalyzer.analyze(TEST_PARSER.parseQuery(query)));
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
            (out, plan) -> out.writeNamedWriteable(plan),
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
        assumePromqlWithoutGrouping();
        var plan = planPromql("PROMQL index=k8s step=1h result=(sum without (pod) (sum by (cluster, region, pod) (network.cost)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", MetadataAttribute.TIMESERIES)));
    }

    public void testNestedWithoutOverByPacksTimeSeriesOnlyOnce() {
        assumePromqlWithoutGrouping();
        LogicalPlan analyzed = tsAnalyzer.analyze(
            TEST_PARSER.parseQuery("PROMQL index=k8s step=1h result=(sum without (pod) (sum by (cluster, region, pod) (network.cost)))")
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
        assumePromqlWithoutGrouping();
        LogicalPlan analyzed = tsAnalyzer.analyze(
            TEST_PARSER.parseQuery("PROMQL index=k8s step=1h result=(sum by (cluster) (sum without (pod) (network.cost)))")
        );
        PromqlCommand promql = analyzed.collect(PromqlCommand.class).getFirst();

        LogicalPlan translated = new TranslatePromqlToEsqlPlan().apply(promql, logicalOptimizerCtx);
        TimeSeriesAggregate innerAggregate = translated.collect(TimeSeriesAggregate.class).getFirst();
        assertTrue(innerAggregate.groupings().stream().anyMatch(grouping -> Alias.unwrap(grouping) instanceof TimeSeriesWithout));
        assertThat(innerAggregate.aggregates().stream().map(NamedExpression::name).toList(), hasItem("cluster"));
    }

    public void testNestedWithoutOverWithoutIsRejectedByVerifier() {
        assumePromqlWithoutGrouping();
        VerificationException e = assertThrows(
            VerificationException.class,
            () -> planPromql("PROMQL index=k8s step=1h result=(sum without (region) (sum without (pod) (network.cost)))")
        );
        assertThat(e.getMessage(), containsString("nested WITHOUT over WITHOUT is not supported at this time"));
    }

    public void testNestedWithoutOverPartialByProducesTimeSeriesOutput() {
        assumePromqlWithoutGrouping();
        var plan = planPromql("PROMQL index=k8s step=1h result=(sum without (pod) (sum by (cluster, pod) (network.cost)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", MetadataAttribute.TIMESERIES)));
    }

    public void testAvgOverWithoutRangeAggregationPlans() {
        assumePromqlWithoutGrouping();
        planPromql("PROMQL index=k8s step=1h result=(avg(sum without (pod, region) (avg_over_time(network.cost[1h]))))");
    }

    public void testSumByRegionOverSumWithoutRegionProducesScalar() {
        assumePromqlWithoutGrouping();
        var plan = planPromql("PROMQL index=k8s step=1h result=(sum by (region) (sum without (region) (network.cost)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem("result"));
        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem("step"));
    }

    public void testWithoutOverByOverWithoutIsRejectedByVerifier() {
        assumePromqlWithoutGrouping();
        VerificationException e = assertThrows(
            VerificationException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1h result=(sum without (region) (sum by (cluster, region) (sum without (pod) (network.cost))))"
            )
        );
        assertThat(e.getMessage(), containsString("nested WITHOUT over WITHOUT is not supported at this time"));
    }

    public void testByOverWithoutOverByProducesExactOutput() {
        assumePromqlWithoutGrouping();
        var plan = planPromql(
            "PROMQL index=k8s step=1h result=(sum by (cluster) (sum without (pod) (sum by (cluster, region, pod) (network.cost))))"
        );
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", "cluster")));
    }

    public void testScalarOverWithoutProducesScalarOutput() {
        assumePromqlWithoutGrouping();
        var plan = planPromql("PROMQL index=k8s step=1h result=(scalar(sum without (pod, region) (avg_over_time(network.cost[1h]))))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
    }

    public void testWithoutAbsentLabelIsNoOp() {
        assumePromqlWithoutGrouping();
        LogicalPlan analyzed = tsAnalyzer.analyze(
            TEST_PARSER.parseQuery("PROMQL index=k8s step=1h result=(sum by (cluster) (sum without (does_not_exist) (network.cost)))")
        );
        PromqlCommand promql = analyzed.collect(PromqlCommand.class).getFirst();
        LogicalPlan translated = new TranslatePromqlToEsqlPlan().apply(promql, logicalOptimizerCtx);
        TimeSeriesAggregate innerTsa = translated.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(innerTsa.aggregates().stream().map(NamedExpression::name).toList(), hasItem("cluster"));
    }

    public void testWithoutAllKnownLabelsProducesEmptyGrouping() {
        assumePromqlWithoutGrouping();
        var plan = planPromql("PROMQL index=k8s step=1h result=(sum without (cluster, region, pod) (network.cost))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", MetadataAttribute.TIMESERIES)));
    }

    public void testWithoutPostfixSyntaxPlans() {
        assumePromqlWithoutGrouping();
        planPromql("PROMQL index=k8s step=1h result=(sum(avg_over_time(network.cost[1h])) without (pod, region))");
    }

    public void testWithoutTrailingCommaPlans() {
        assumePromqlWithoutGrouping();
        planPromql("PROMQL index=k8s step=1h result=(sum without (pod, region,) (avg_over_time(network.cost[1h])))");
    }

    public void testWithoutEmptyLabelListPlans() {
        assumePromqlWithoutGrouping();
        LogicalPlan analyzed = tsAnalyzer.analyze(
            TEST_PARSER.parseQuery("PROMQL index=k8s step=1h result=(sum by (cluster) (sum without () (avg_over_time(network.cost[1h]))))")
        );
        PromqlCommand promql = analyzed.collect(PromqlCommand.class).getFirst();
        LogicalPlan translated = new TranslatePromqlToEsqlPlan().apply(promql, logicalOptimizerCtx);
        TimeSeriesAggregate innerTsa = translated.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(innerTsa.aggregates().stream().map(NamedExpression::name).toList(), hasItem("cluster"));
    }

    public void testScalarOverMaxOfWithoutProducesScalarOutput() {
        assumePromqlWithoutGrouping();
        var plan = planPromql("PROMQL index=k8s step=1h result=(scalar(max(sum without (pod, region) (avg_over_time(network.cost[1h])))))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
    }
}
