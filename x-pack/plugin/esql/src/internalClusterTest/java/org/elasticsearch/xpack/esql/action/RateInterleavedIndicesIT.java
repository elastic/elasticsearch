/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.EsqlTestUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;

public class RateInterleavedIndicesIT extends AbstractEsqlIntegTestCase {

    public void testRateForFirstIndex() {
        TestData data = createTestData();
        assertRate("rate-a", data.samplesA());
    }

    public void testRateForSecondIndex() {
        TestData data = createTestData();
        assertRate("rate-b", data.samplesB());
    }

    public void testRateAcrossInterleavedIndices() {
        TestData data = createTestData();
        assertRate("rate-*", data.allSamples());
    }

    private TestData createTestData() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String nodeA = randomDataNode().getName();
        String nodeB = randomValueOtherThan(nodeA, () -> randomDataNode().getName());

        createTimeSeriesIndex("rate-a", nodeA);
        createTimeSeriesIndex("rate-b", nodeB);

        long start = System.currentTimeMillis();
        List<Sample> samples = List.of(
            new Sample(start + 1_000L, 1),
            new Sample(start + 2_000L, 0),
            new Sample(start + 3_000L, 1),
            new Sample(start + 4_000L, 0),
            new Sample(start + 5_000L, 1),
            new Sample(start + 6_000L, 0),
            new Sample(start + 7_000L, 1),
            new Sample(start + 8_000L, 0)
        );
        List<Sample> samplesA = new ArrayList<>();
        List<Sample> samplesB = new ArrayList<>();
        for (int i = 0; i < samples.size(); i++) {
            Sample sample = samples.get(i);
            List<Sample> indexSamples = i % 2 == 0 ? samplesA : samplesB;
            String index = i % 2 == 0 ? "rate-a" : "rate-b";
            indexSamples.add(sample);
            indexSample(index, sample);
        }
        refresh("rate-a", "rate-b");
        return new TestData(samples, samplesA, samplesB);
    }

    private void createTimeSeriesIndex(String index, String node) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("mode", "time_series")
                        .putList("routing_path", List.of("host"))
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", node)
                )
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "request_count",
                    "type=long,time_series_metric=counter"
                )
        );
    }

    private void indexSample(String index, Sample sample) {
        client().prepareIndex(index).setSource("@timestamp", sample.timestamp(), "host", "h1", "request_count", sample.value()).get();
    }

    private double rate(String indexPattern) {
        try (var response = run("TS " + indexPattern + " | STATS result = max(rate(request_count)) BY host")) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(response);
            assertThat(rows, hasSize(1));
            return (double) rows.get(0).get(0);
        }
    }

    private void assertRate(String indexPattern, List<Sample> samples) {
        assertThat(rate(indexPattern), closeTo(referenceRate(samples), 1e-12));
    }

    private static double referenceRate(List<Sample> samples) {
        List<Sample> sorted = samples.stream().sorted(Comparator.comparingLong(Sample::timestamp)).toList();
        double increase = 0;
        for (int i = 1; i < sorted.size(); i++) {
            long previous = sorted.get(i - 1).value();
            long current = sorted.get(i).value();
            increase += current < previous ? current : current - previous;
        }
        return increase * 1_000 / (sorted.getLast().timestamp() - sorted.getFirst().timestamp());
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }

    private record Sample(long timestamp, long value) {}

    private record TestData(List<Sample> allSamples, List<Sample> samplesA, List<Sample> samplesB) {}
}
