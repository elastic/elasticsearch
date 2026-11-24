/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.LoadMapping;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.AbstractTimeSeriesMetadataFunction;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.logicalOptimizerContext;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A cluster of nodes of the release before the label container functions and newer ones keeps running every {@code PROMQL}
 * query that release ran: the queries of its csv-spec files, frozen in {@code promql-9.6-queries.txt}. A newer coordinator
 * translates them the same way at any version and runs the label container functions of the plan itself, so only what reaches
 * the older nodes matters: the data node fragments must not call a label container function, which the older nodes do not
 * know.
 * <p>
 * The right side of a {@link HashJoin} runs as a plan of its own before the join, which then reads its rows from a local
 * relation, so its fragments are checked as such. A query over an index pattern resolves the merged mappings of several
 * indices, which only the field capabilities of a running cluster give; the mixed cluster csv-spec suite runs those.
 */
public class PromqlMixedClusterPlanTests extends AbstractPromqlPlanOptimizerTests {

    /** The version before the label container functions: the minimum version of a cluster with a node that does not know them. */
    private static final TransportVersion MIXED = TransportVersionUtils.getPreviousVersion(
        AbstractTimeSeriesMetadataFunction.ESQL_TIMESERIES_METADATA_FUNCTIONS
    );

    /** The time-series indices the queries read besides the ones {@link #tsAnalyzer()} holds, by name and mapping file. */
    private static final List<List<String>> INDICES = List.of(
        List.of("prometheus-k8s", "prometheus-k8s-mappings.json"),
        List.of("exp_histo_sample", "exp_histo_sample-mappings.json"),
        List.of("promql_classic_histogram", "mapping-promql-classic-histogram-passthrough.json"),
        List.of("promql_histogram_no_le", "mapping-promql-histogram-no-le-passthrough.json"),
        List.of("metric_temporality", "metric_temporality-mappings.json"),
        List.of("k8s-downsampled", "k8s-downsampled-mappings.json"),
        List.of("prom-metrics-name", "prom-metrics-name-mappings.json"),
        List.of("prom-metrics", "prom-metrics-mappings.json"),
        List.of("metrics-cpu", "otel-exemplar-source-metrics-mappings.json")
    );

    public PromqlMixedClusterPlanTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testQueriesOfThePreviousReleaseKeepLabelContainerFunctionsOffDataNodes() throws IOException {
        assumeTrue("the test pins the version of a mixed cluster itself", versionMode == VersionMode.CURRENT);
        TestAnalyzer analyzer = mixedClusterAnalyzer();
        var optimizer = new LogicalPlanOptimizer(logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), MIXED));
        List<String> failures = new ArrayList<>();
        int planned = 0;
        for (String line : queries()) {
            String[] test = line.split(" ", 2);
            String query = test[1].replace("$now-1h", "\"2024-05-10T00:00:00Z\"").replace("$now", "\"2024-05-10T01:00:00Z\"");
            if (query.matches(".*index=\\S*[,*].*")) {
                // an index pattern, which the mixed cluster csv-spec suite runs
                continue;
            }
            LogicalPlan plan;
            try {
                plan = optimizer.optimize(analyzer.query(query));
            } catch (RuntimeException e) {
                failures.add(test[0] + " fails: " + String.valueOf(e.getMessage()).lines().findFirst().orElse(""));
                continue;
            }
            for (LogicalPlan fragment : dataNodeFragments(plan)) {
                fragment.forEachExpressionDown(
                    AbstractTimeSeriesMetadataFunction.class,
                    function -> failures.add(test[0] + " sends [" + function.sourceText() + "] to a data node")
                );
            }
            planned++;
        }
        assertThat(String.join("\n", failures), failures, empty());
        assertThat("queries planned", planned, greaterThan(1000));
    }

    /** The logical plans a coordinator sends to the data nodes to run {@code plan}. */
    private static List<LogicalPlan> dataNodeFragments(LogicalPlan plan) {
        List<LogicalPlan> fragments = new ArrayList<>();
        LogicalPlan main = plan.transformUp(HashJoin.class, join -> {
            fragments.addAll(dataNodeFragments(join.right()));
            LogicalPlan rows = new LocalRelation(join.right().source(), join.right().output(), EmptyLocalSupplier.EMPTY);
            return join.replaceChildren(join.left(), rows);
        });
        var physical = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(EsqlTestUtils.TEST_CFG, MIXED)).optimize(
            new Mapper().map(new Versioned<>(main, MIXED))
        );
        physical.forEachDown(FragmentExec.class, fragment -> fragments.add(fragment.fragment()));
        return fragments;
    }

    private TestAnalyzer mixedClusterAnalyzer() throws IOException {
        TestAnalyzer analyzer = tsAnalyzer();
        for (List<String> index : INDICES) {
            analyzer = analyzer.addIndex(timeSeriesIndex(index.get(0), index.get(1)));
        }
        return analyzer.minimumTransportVersion(MIXED);
    }

    private static List<String> queries() throws IOException {
        try (
            InputStream in = PromqlMixedClusterPlanTests.class.getResourceAsStream("promql-9.6-queries.txt");
            var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
        ) {
            return reader.lines().filter(line -> line.isBlank() == false && line.startsWith("#") == false).toList();
        }
    }

    /**
     * A time-series index as the field capabilities of a cluster give it: a field of a {@code passthrough} object is a field of
     * its own name too, the object of the higher priority winning, and a dimension {@code passthrough} object makes its fields
     * dimensions.
     */
    @SuppressWarnings("unchecked")
    private static EsIndex timeSeriesIndex(String name, String mappingFile) throws IOException {
        Map<String, Object> mapping;
        try (InputStream in = LoadMapping.class.getResourceAsStream("/index/mappings/" + mappingFile)) {
            assertNotNull(mappingFile, in);
            mapping = XContentHelper.convertToMap(JsonXContent.jsonXContent, in, true);
        }
        var properties = new LinkedHashMap<>((Map<String, Object>) mapping.get("properties"));
        List<String> passthroughs = properties.entrySet()
            .stream()
            .filter(field -> field.getValue() instanceof Map<?, ?> content && "passthrough".equals(content.get("type")))
            .sorted(
                Comparator.comparingInt(
                    field -> -((Number) ((Map<String, Object>) field.getValue()).getOrDefault("priority", 0)).intValue()
                )
            )
            .map(Map.Entry::getKey)
            .toList();
        var byOwnName = new LinkedHashMap<String, Object>();
        for (String passthrough : passthroughs) {
            var content = new LinkedHashMap<>((Map<String, Object>) properties.get(passthrough));
            boolean dimension = Boolean.TRUE.equals(content.remove("time_series_dimension"));
            content.remove("priority");
            content.put("type", "object");
            var fields = new LinkedHashMap<String, Object>();
            for (var field : ((Map<String, Object>) content.get("properties")).entrySet()) {
                var fieldMapping = new LinkedHashMap<>((Map<String, Object>) field.getValue());
                if (dimension) {
                    fieldMapping.put("time_series_dimension", true);
                }
                fields.put(field.getKey(), fieldMapping);
                byOwnName.putIfAbsent(field.getKey(), fieldMapping);
            }
            content.put("properties", fields);
            properties.put(passthrough, content);
        }
        byOwnName.forEach(properties::putIfAbsent);
        String json = Strings.toString(XContentFactory.jsonBuilder().map(Map.of("properties", properties)));
        return new EsIndex(
            name,
            LoadMapping.loadMapping(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))),
            Map.of(name, new IndexProperties(IndexMode.TIME_SERIES, 0)),
            Map.of(),
            Map.of()
        );
    }
}
