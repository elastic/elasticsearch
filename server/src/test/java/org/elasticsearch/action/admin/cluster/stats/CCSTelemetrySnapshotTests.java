/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.admin.cluster.stats.CCSTelemetrySnapshot.PerClusterCCSTelemetry;
import org.elasticsearch.action.admin.cluster.stats.LongMetric.LongMetricValue;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class CCSTelemetrySnapshotTests extends AbstractWireSerializingTestCase<CCSTelemetrySnapshot> {

    private LongMetricValue randomLongMetricValue() {
        LongMetric v = new LongMetric();
        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            v.record(randomIntBetween(0, 1_000_000));
        }
        return v.getValue();
    }

    private PerClusterCCSTelemetry randomPerClusterCCSTelemetry() {
        return new PerClusterCCSTelemetry(randomLongBetween(0, 1_000_000), randomLongBetween(0, 1_000_000), randomLongMetricValue());
    }

    @Override
    protected CCSTelemetrySnapshot createTestInstance() {
        if (randomBoolean()) {
            return new CCSTelemetrySnapshot();
        } else {
            return randomCCSTelemetrySnapshot();
        }
    }

    private CCSTelemetrySnapshot randomCCSTelemetrySnapshot() {
        return new CCSTelemetrySnapshot(
            randomLongBetween(0, 1_000_000),
            randomLongBetween(0, 1_000_000),
            Map.of(),
            randomLongMetricValue(),
            randomLongMetricValue(),
            randomLongMetricValue(),
            randomLongBetween(0, 1_000_000),
            randomDoubleBetween(0.0, 100.0, false),
            randomLongBetween(0, 1_000_000),
            Map.of(),
            Map.of(),
            randomMap(1, 10, () -> new Tuple<>(randomAlphaOfLengthBetween(5, 10), randomPerClusterCCSTelemetry()))
        );
    }

    @Override
    protected Writeable.Reader<CCSTelemetrySnapshot> instanceReader() {
        return CCSTelemetrySnapshot::new;
    }

    @Override
    protected CCSTelemetrySnapshot mutateInstance(CCSTelemetrySnapshot instance) throws IOException {
        // create a copy of CCSTelemetrySnapshot by extracting each field and mutating it
        long totalCount = instance.getTotalCount();
        long successCount = instance.getSuccessCount();
        var failureReasons = instance.getFailureReasons();
        LongMetricValue took = instance.getTook();
        LongMetricValue tookMrtTrue = instance.getTookMrtTrue();
        LongMetricValue tookMrtFalse = instance.getTookMrtFalse();
        long skippedRemotes = instance.getSearchCountWithSkippedRemotes();
        long remotesPerSearchMax = instance.getRemotesPerSearchMax();
        double remotesPerSearchAvg = instance.getRemotesPerSearchAvg();
        var featureCounts = instance.getFeatureCounts();
        var clientCounts = instance.getClientCounts();
        var perClusterCCSTelemetries = instance.getByRemoteCluster();

        // Mutate values
        int i = randomInt(11);
        switch (i) {
            case 0:
                totalCount += randomNonNegativeLong();
                break;
            case 1:
                successCount += randomNonNegativeLong();
                break;
            case 2:
                failureReasons = new HashMap<>(failureReasons);
                if (failureReasons.isEmpty() || randomBoolean()) {
                    failureReasons.put(randomAlphaOfLengthBetween(5, 10), randomNonNegativeLong());
                } else {
                    // modify random element of the map
                    String key = randomFrom(failureReasons.keySet());
                    failureReasons.put(key, randomNonNegativeLong());
                }
                break;
            case 3:
                took = randomLongMetricValue();
                break;
            case 4:
                tookMrtTrue = randomLongMetricValue();
                break;
            case 5:
                tookMrtFalse = randomLongMetricValue();
                break;
            case 6:
                skippedRemotes += randomNonNegativeLong();
                break;
            case 7:
                remotesPerSearchMax += randomNonNegativeLong();
                break;
            case 8:
                remotesPerSearchAvg = randomDoubleBetween(0.0, 100.0, false);
                break;
            case 9:
                featureCounts = new HashMap<>(featureCounts);
                if (featureCounts.isEmpty() || randomBoolean()) {
                    featureCounts.put(randomAlphaOfLengthBetween(5, 10), randomNonNegativeLong());
                } else {
                    // modify random element of the map
                    String key = randomFrom(featureCounts.keySet());
                    featureCounts.put(key, randomNonNegativeLong());
                }
                break;
            case 10:
                clientCounts = new HashMap<>(clientCounts);
                if (clientCounts.isEmpty() || randomBoolean()) {
                    clientCounts.put(randomAlphaOfLengthBetween(5, 10), randomNonNegativeLong());
                } else {
                    // modify random element of the map
                    String key = randomFrom(clientCounts.keySet());
                    clientCounts.put(key, randomNonNegativeLong());
                }
                break;
            case 11:
                perClusterCCSTelemetries = new HashMap<>(perClusterCCSTelemetries);
                if (perClusterCCSTelemetries.isEmpty() || randomBoolean()) {
                    perClusterCCSTelemetries.put(randomAlphaOfLengthBetween(5, 10), randomPerClusterCCSTelemetry());
                } else {
                    // modify random element of the map
                    String key = randomFrom(perClusterCCSTelemetries.keySet());
                    perClusterCCSTelemetries.put(key, randomPerClusterCCSTelemetry());
                }
                break;
        }
        // Return new instance
        return new CCSTelemetrySnapshot(
            totalCount,
            successCount,
            failureReasons,
            took,
            tookMrtTrue,
            tookMrtFalse,
            remotesPerSearchMax,
            remotesPerSearchAvg,
            skippedRemotes,
            featureCounts,
            clientCounts,
            perClusterCCSTelemetries
        );
    }

    public void testAdd() {
        CCSTelemetrySnapshot empty = new CCSTelemetrySnapshot();
        CCSTelemetrySnapshot full = randomCCSTelemetrySnapshot();
        empty.add(full);
        assertThat(empty, equalTo(full));
        // Add again
        empty.add(full);
        assertThat(empty.getTotalCount(), equalTo(full.getTotalCount() * 2));
        assertThat(empty.getSuccessCount(), equalTo(full.getSuccessCount() * 2));
        // check that each element of the map is doubled
        empty.getFailureReasons().forEach((k, v) -> assertThat(v, equalTo(full.getFailureReasons().get(k) * 2)));
        assertThat(empty.getTook().count(), equalTo(full.getTook().count() * 2));
        assertThat(empty.getTookMrtTrue().count(), equalTo(full.getTookMrtTrue().count() * 2));
        assertThat(empty.getTookMrtFalse().count(), equalTo(full.getTookMrtFalse().count() * 2));
        assertThat(empty.getSearchCountWithSkippedRemotes(), equalTo(full.getSearchCountWithSkippedRemotes() * 2));
        assertThat(empty.getRemotesPerSearchMax(), equalTo(full.getRemotesPerSearchMax()));
        assertThat(empty.getRemotesPerSearchAvg(), closeTo(full.getRemotesPerSearchAvg(), 0.01));
        empty.getFeatureCounts().forEach((k, v) -> assertThat(v, equalTo(full.getFeatureCounts().get(k) * 2)));
        empty.getClientCounts().forEach((k, v) -> assertThat(v, equalTo(full.getClientCounts().get(k) * 2)));
        empty.getByRemoteCluster().forEach((k, v) -> {
            assertThat(v.getCount(), equalTo(full.getByRemoteCluster().get(k).getCount() * 2));
            assertThat(v.getSkippedCount(), equalTo(full.getByRemoteCluster().get(k).getSkippedCount() * 2));
            assertThat(v.getTook().count(), equalTo(full.getByRemoteCluster().get(k).getTook().count() * 2));
        });
    }

    public void testAddTwo() {
        CCSTelemetrySnapshot empty = new CCSTelemetrySnapshot();
        CCSTelemetrySnapshot full = randomCCSTelemetrySnapshot();
        CCSTelemetrySnapshot full2 = randomCCSTelemetrySnapshot();

        empty.add(full);
        empty.add(full2);
        assertThat(empty.getTotalCount(), equalTo(full.getTotalCount() + full2.getTotalCount()));
        assertThat(empty.getSuccessCount(), equalTo(full.getSuccessCount() + full2.getSuccessCount()));
        empty.getFailureReasons()
            .forEach(
                (k, v) -> assertThat(
                    v,
                    equalTo(full.getFailureReasons().getOrDefault(k, 0L) + full2.getFailureReasons().getOrDefault(k, 0L))
                )
            );
        assertThat(empty.getTook().count(), equalTo(full.getTook().count() + full2.getTook().count()));
        assertThat(empty.getTookMrtTrue().count(), equalTo(full.getTookMrtTrue().count() + full2.getTookMrtTrue().count()));
        assertThat(empty.getTookMrtFalse().count(), equalTo(full.getTookMrtFalse().count() + full2.getTookMrtFalse().count()));
        assertThat(
            empty.getSearchCountWithSkippedRemotes(),
            equalTo(full.getSearchCountWithSkippedRemotes() + full2.getSearchCountWithSkippedRemotes())
        );
        assertThat(empty.getRemotesPerSearchMax(), equalTo(Math.max(full.getRemotesPerSearchMax(), full2.getRemotesPerSearchMax())));
        double expectedAvg = (full.getRemotesPerSearchAvg() * full.getTotalCount() + full2.getRemotesPerSearchAvg() * full2.getTotalCount())
            / empty.getTotalCount();
        assertThat(empty.getRemotesPerSearchAvg(), closeTo(expectedAvg, 0.01));
        empty.getFeatureCounts()
            .forEach(
                (k, v) -> assertThat(v, equalTo(full.getFeatureCounts().getOrDefault(k, 0L) + full2.getFeatureCounts().getOrDefault(k, 0L)))
            );
        empty.getClientCounts()
            .forEach(
                (k, v) -> assertThat(v, equalTo(full.getClientCounts().getOrDefault(k, 0L) + full2.getClientCounts().getOrDefault(k, 0L)))
            );
        PerClusterCCSTelemetry zeroDummy = new PerClusterCCSTelemetry();
        empty.getByRemoteCluster().forEach((k, v) -> {
            assertThat(
                v.getCount(),
                equalTo(
                    full.getByRemoteCluster().getOrDefault(k, zeroDummy).getCount() + full2.getByRemoteCluster()
                        .getOrDefault(k, zeroDummy)
                        .getCount()
                )
            );
            assertThat(
                v.getSkippedCount(),
                equalTo(
                    full.getByRemoteCluster().getOrDefault(k, zeroDummy).getSkippedCount() + full2.getByRemoteCluster()
                        .getOrDefault(k, zeroDummy)
                        .getSkippedCount()
                )
            );
            assertThat(
                v.getTook().count(),
                equalTo(
                    full.getByRemoteCluster().getOrDefault(k, zeroDummy).getTook().count() + full2.getByRemoteCluster()
                        .getOrDefault(k, zeroDummy)
                        .getTook()
                        .count()
                )
            );
        });
    }

    private LongMetricValue manyValuesHistogram(long startingWith) {
        LongMetric metric = new LongMetric();
        // Produce 100 values from startingWith to 2 * startingWith with equal intervals
        // We need to space values relative to initial value, otherwise the histogram would put them all in one bucket
        for (long i = startingWith; i < 2 * startingWith; i += startingWith / 100) {
            metric.record(i);
        }
        return metric.getValue();
    }

    public void testToXContent() throws IOException {
        long totalCount = 10;
        long successCount = 20;
        // Using TreeMap's here to ensure consistent ordering in the JSON output
        var failureReasons = new TreeMap<>(Map.of("reason1", 1L, "reason2", 2L, "unknown", 3L));
        LongMetricValue took = manyValuesHistogram(1000);
        LongMetricValue tookMrtTrue = manyValuesHistogram(5000);
        LongMetricValue tookMrtFalse = manyValuesHistogram(10000);
        long skippedRemotes = 5;
        long remotesPerSearchMax = 6;
        double remotesPerSearchAvg = 7.89;
        var featureCounts = new TreeMap<>(Map.of("async", 10L, "mrt", 20L, "wildcard", 30L));
        var clientCounts = new TreeMap<>(Map.of("kibana", 40L, "other", 500L));
        var perClusterCCSTelemetries = new TreeMap<>(
            Map.of(
                "",
                new PerClusterCCSTelemetry(12, 0, manyValuesHistogram(2000)),
                "remote1",
                new PerClusterCCSTelemetry(100, 22, manyValuesHistogram(2000)),
                "remote2",
                new PerClusterCCSTelemetry(300, 42, manyValuesHistogram(500000))
            )
        );

        var snapshot = new CCSTelemetrySnapshot(
            totalCount,
            successCount,
            failureReasons,
            took,
            tookMrtTrue,
            tookMrtFalse,
            remotesPerSearchMax,
            remotesPerSearchAvg,
            skippedRemotes,
            featureCounts,
            clientCounts,
            perClusterCCSTelemetries
        );
        String expected = readJSONFromResource("telemetry_test.json");
        assertEquals(expected, snapshot.toString());
    }

    private String readJSONFromResource(String fileName) throws IOException {
        try (InputStream inputStream = getClass().getResourceAsStream("/org/elasticsearch/action/admin/cluster/stats/" + fileName)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + fileName);
            }
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
