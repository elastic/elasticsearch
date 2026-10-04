/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Pins how a translation represents a series' identity to the cluster's minimum transport version. Before
 * {@link TimeSeriesUnset#ESQL_TIMESERIES_METADATA_UNSET} it asks the source for one {@code _timeseries} per exclusion set and
 * emits no {@link TimeSeriesUnset}, so older nodes never see one. From that version on each node carries its series' one whole
 * {@code _timeseries}, and the labels an identity drops are unset from it with {@link TimeSeriesUnset}.
 */
public class PromqlTimeSeriesUnsetTranslationTests extends AbstractPromqlPlanOptimizerTests {

    private static final String RANGE = "PROMQL index=k8s step=1h start=\"2024-05-10T00:00:00.000Z\" end=\"2024-05-10T01:00:00.000Z\" ";

    public PromqlTimeSeriesUnsetTranslationTests(VersionMode versionMode) {
        super(versionMode);
    }

    private static List<String> queries() {
        List<String> queries = new ArrayList<>(
            List.of(
                RANGE + "result=(network.bytes_in)",
                RANGE + "result=(rate(network.total_bytes_in[5m]))",
                RANGE + "result=(sum without (pod) (network.bytes_in))",
                RANGE + "result=(sum without (pod, region) (rate(network.total_bytes_in[5m])))",
                RANGE + "result=(sum by (cluster) (sum without (pod) (network.cost)))",
                RANGE + "result=(topk(2, network.bytes_in))",
                RANGE + "result=(max by (cluster) (topk by (pod) (1, network.cost)))",
                RANGE + "result=(network.bytes_in or network.cost)",
                RANGE + "result=(network.bytes_in * 8)"
            )
        );
        if (EsqlCapabilities.Cap.PROMQL_LABEL_FUNCTIONS.isEnabled()) {
            queries.add(RANGE + "result=(sum by (tier) (label_replace(network.bytes_in, \"tier\", \"$1\", \"region\", \"(.+)\")))");
        }
        if (EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled()) {
            queries.add(
                RANGE + "result=(sum by (cluster, pod) (network.bytes_in) / ignoring (pod) group_left sum by (cluster) (network.cost))"
            );
        }
        return queries;
    }

    private LogicalPlan analyze(String query, TransportVersion minimumVersion) {
        return tsAnalyzer().minimumTransportVersion(minimumVersion).query(query);
    }

    public void testNoTimeSeriesUnsetBeforeTheVersion() {
        TransportVersion older = TransportVersionUtils.getPreviousVersion(TimeSeriesUnset.ESQL_TIMESERIES_METADATA_UNSET);
        for (String query : queries()) {
            assertThat(query, unsets(analyze(query, older)), empty());
        }
    }

    public void testOneWholeTimeSeriesPerAggregateFromTheVersion() {
        for (TransportVersion version : List.of(TimeSeriesUnset.ESQL_TIMESERIES_METADATA_UNSET, TransportVersion.current())) {
            for (String query : queries()) {
                LogicalPlan plan = analyze(query, version);
                for (TimeSeriesAggregate aggregate : plan.collect(TimeSeriesAggregate.class)) {
                    List<TimeSeriesWithout> loaded = new ArrayList<>();
                    aggregate.groupings().forEach(grouping -> grouping.forEachDown(TimeSeriesWithout.class, loaded::add));
                    assertThat(query, loaded.size(), lessThanOrEqualTo(1));
                    loaded.forEach(timeseries -> assertThat(query, timeseries.children(), empty()));
                }
                LogicalPlan optimized = logicalOptimizerWithLatestVersion.optimize(plan);
                for (EsRelation relation : optimized.collect(EsRelation.class)) {
                    List<TimeSeriesMetadataAttribute> loaded = sourceRecords(relation);
                    assertThat(query, loaded.size(), lessThanOrEqualTo(1));
                    loaded.forEach(timeseries -> assertThat(query, timeseries.excludedFields(), empty()));
                }
            }
        }
    }

    public void testWithoutUnsetsItsLabels() {
        LogicalPlan plan = analyze(RANGE + "result=(sum without (pod, region) (network.bytes_in))", TransportVersion.current());
        assertThat(unsetDimensions(plan), hasItem("pod"));
        assertThat(unsetDimensions(plan), hasItem("region"));
    }

    /** A label is unset under the field names that store it, and nothing else. */
    public void testWithoutUnsetsTheStoredNamesOfItsLabels() {
        LogicalPlan k8s = analyze(RANGE + "result=(sum without (pod) (network.bytes_in))", TransportVersion.current());
        assertThat(unsetDimensions(k8s), equalTo(Set.of("pod")));
        // a Prometheus label is stored under the `labels.` passthrough, which the plan resolves itself
        LogicalPlan prometheus = currentAnalyzer().addIndex(prometheusMetrics())
            .query("PROMQL index=prom-metrics step=1h result=(sum without (cpu) (metrics.cpu_time))");
        assertThat(unsetDimensions(prometheus), equalTo(Set.of("cpu", "labels.cpu")));
    }

    /**
     * A label that names a field while another dimension ends in it, as an OTel passthrough alias does ({@code cpu} for
     * {@code attributes.cpu}), may name more fields than the plan can tell. The command keeps one {@code _timeseries} per
     * exclusion set, and the source resolves the label per shard.
     */
    public void testLabelThePlanCannotResolveIsExcludedAtTheSource() {
        for (String label : List.of("cpu", "host.name")) {
            LogicalPlan plan = analyze(
                "PROMQL index=otel-metrics step=1h result=(sum without (" + label + ") (metrics.system.cpu.time))",
                TransportVersion.current()
            );
            assertThat(label, unsets(plan), empty());
            List<TimeSeriesMetadataAttribute> loaded = sourceRecords(logicalOptimizerWithLatestVersion.optimize(plan));
            assertThat(label, loaded, hasSize(1));
            assertThat(label, loaded.getFirst().excludedFields(), hasItem(label));
        }
    }

    /** A constant child has no series identity: the aggregate returns it without unsetting anything. */
    public void testWithoutOverAConstantUnsetsNothing() {
        assertThat(unsets(analyze(RANGE + "result=(sum without (pod) (vector(1)))", TransportVersion.current())), empty());
    }

    /** An empty label list drops nothing: the per-exclusion-set translation on every version, identical plans. */
    public void testWithoutAnEmptyLabelListPlansAsBefore() {
        String query = RANGE + "result=(max without () (network.cost))";
        LogicalPlan before = analyze(query, TransportVersionUtils.getPreviousVersion(TimeSeriesUnset.ESQL_TIMESERIES_METADATA_UNSET));
        LogicalPlan after = analyze(query, TransportVersion.current());
        assertThat(unsets(after), empty());
        assertThat(withoutIds(after.toString()), equalTo(withoutIds(before.toString())));
    }

    /** A child of promoted labels only carries no _timeseries: there is nothing to unset, the promoted labels regroup. */
    public void testWithoutOverPromotedLabelsUnsetsNothing() {
        LogicalPlan plan = analyze(RANGE + "result=(sum without (pod) (sum by (cluster, pod) (network.cost)))", TransportVersion.current());
        assertThat(unsets(plan), empty());
        assertThat(sourceRecords(logicalOptimizerWithLatestVersion.optimize(plan)), empty());
    }

    /** The le of a classic histogram is unset where the buckets merge, unless they were already regrouped by promoted labels. */
    public void testHistogramUnsetsLeFromBucketsCarryingTheTimeSeries() {
        String histogram = "PROMQL index=prom_hist step=1m result=(histogram_quantile(0.9, ";
        LogicalPlan raw = histogramAnalyzer().query(histogram + "request_duration_seconds_bucket))");
        assertThat(unsetDimensions(raw), equalTo(Set.of("le")));
        LogicalPlan rate = histogramAnalyzer().query(histogram + "rate(request_duration_seconds_bucket[5m])))");
        assertThat(unsetDimensions(rate), equalTo(Set.of("le")));
        LogicalPlan promoted = histogramAnalyzer().query(histogram + "sum by (job, le) (rate(request_duration_seconds_bucket[5m]))))");
        assertThat(unsets(promoted), empty());
    }

    /**
     * A {@code without} over a classic histogram unsets its labels from the {@code _timeseries} the histogram already regrouped
     * with {@code le} unset: one whole {@code _timeseries} loaded, edited twice. The per-exclusion-set translation would need
     * two {@code _timeseries}, one per exclusion set, on one node.
     */
    public void testWithoutOverAHistogramUnsetsFromTheRegroupedTimeSeries() {
        LogicalPlan plan = histogramAnalyzer().query(
            "PROMQL index=prom_hist step=1m result=(max without (instance) "
                + "(histogram_quantile(0.9, rate(request_duration_seconds_bucket[5m]))))"
        );
        assertThat(
            unsets(plan).stream().map(TimeSeriesUnset::dimensionNames).toList(),
            containsInAnyOrder(List.of("le"), List.of("instance"))
        );
        List<TimeSeriesMetadataAttribute> loaded = sourceRecords(logicalOptimizerWithLatestVersion.optimize(plan));
        assertThat(loaded, hasSize(1));
        assertThat(loaded.getFirst().excludedFields(), empty());
    }

    private TestAnalyzer histogramAnalyzer() {
        return currentAnalyzer().addIndex("prom_hist", "mapping-promql-classic-histogram.json", IndexMode.TIME_SERIES);
    }

    /** The plan's text without attribute ids; the text wraps long lines at a fixed width, so it is joined first. */
    private static String withoutIds(String plan) {
        return plan.replace("\n", "").replaceAll("#\\d+", "#_");
    }

    /** Branches carrying their _timeseries as loaded compare as they always have. */
    public void testUnionOfLoadedBranchesKeepsThem() {
        LogicalPlan plan = analyze(RANGE + "result=(network.bytes_in or network.cost)", TransportVersion.current());
        assertThat(unsets(plan), empty());
    }

    /** Once a branch edits its _timeseries, every branch rewrites it in canonical form, so the same labels compare equal. */
    public void testUnionCanonicalizesEveryBranchOnceOneIsEdited() {
        LogicalPlan plan = currentAnalyzer().addIndex("prom_hist", "mapping-promql-classic-histogram.json", IndexMode.TIME_SERIES)
            .query(
                "PROMQL index=prom_hist step=1m result=(histogram_quantile(0.9, rate(request_duration_seconds_bucket[5m]))"
                    + " or rate(request_duration_seconds_bucket[5m]))"
            );
        UnionAll union = plan.collect(UnionAll.class).getFirst();
        for (LogicalPlan branch : union.children()) {
            assertThat(branch.toString(), unsets(branch).stream().filter(unset -> unset.dimensions().isEmpty()).toList(), hasSize(1));
        }
    }

    private TestAnalyzer currentAnalyzer() {
        return analyzerWithEnrichPolicies().minimumTransportVersion(TransportVersion.current());
    }

    /** A Prometheus index as field caps reports it: the label {@code labels.cpu} of the passthrough, and its alias {@code cpu}. */
    private static EsIndex prometheusMetrics() {
        var labels = new LinkedHashMap<String, EsField>();
        labels.put("cpu", dimension("cpu"));
        var metrics = new LinkedHashMap<String, EsField>();
        metrics.put("cpu_time", new EsField("cpu_time", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.METRIC));
        var mapping = new LinkedHashMap<String, EsField>();
        mapping.put("@timestamp", DateEsField.dateEsField("@timestamp", Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("labels", new EsField("labels", DataType.OBJECT, labels, false, EsField.TimeSeriesFieldType.NONE));
        mapping.put("cpu", dimension("cpu"));
        mapping.put("metrics", new EsField("metrics", DataType.OBJECT, metrics, false, EsField.TimeSeriesFieldType.NONE));
        var properties = Map.of("prom-metrics", new IndexProperties(IndexMode.TIME_SERIES, 0));
        return new EsIndex("prom-metrics", mapping, properties, Map.of(), Map.of());
    }

    private static EsField dimension(String name) {
        return new KeywordEsField(name, Map.of(), true, 0, false, false, EsField.TimeSeriesFieldType.DIMENSION);
    }

    /**
     * The data-node half of a translation serializes at the version that introduced {@link TimeSeriesUnset}. Unions and vector
     * matches plan one exchange per branch, so only the single-branch queries split into one data-node half.
     */
    public void testDataNodePlanSerializesFromTheVersion() {
        TransportVersion version = TimeSeriesUnset.ESQL_TIMESERIES_METADATA_UNSET;
        int serialized = 0;
        for (String query : queries().stream().filter(q -> q.contains(" or ") == false && q.contains("ignoring") == false).toList()) {
            LogicalPlan optimized = logicalOptimizerWithLatestVersion.optimize(analyze(query, version));
            PhysicalPlan physical = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(EsqlTestUtils.TEST_CFG, version)).optimize(
                new Mapper().map(new Versioned<>(optimized, version))
            );
            PhysicalPlan dataNodePlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(physical, EsqlTestUtils.TEST_CFG).v2();
            if (dataNodePlan == null) {
                continue; // folded to a local relation: nothing ships to a data node
            }
            serialized++;
            SerializationTestUtils.serializeDeserialize(
                dataNodePlan,
                StreamOutput::writeNamedWriteable,
                in -> in.readNamedWriteable(PhysicalPlan.class),
                EsqlTestUtils.TEST_CFG
            );
        }
        assertThat(serialized, greaterThan(0));
        assertThat(unsets(analyze(queries().get(2), version)), not(empty()));
    }

    private static List<TimeSeriesUnset> unsets(LogicalPlan plan) {
        List<TimeSeriesUnset> unsets = new ArrayList<>();
        plan.forEachExpressionDown(TimeSeriesUnset.class, unsets::add);
        return unsets;
    }
}
