/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.metrics.Counters;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.HealthApiFeatureSetUsage.enrichUsageStatsWithValues;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class HealthApiFeatureSetUsageTests extends AbstractWireSerializingTestCase<HealthApiFeatureSetUsage> {

    @Override
    protected HealthApiFeatureSetUsage createTestInstance() {
        return new HealthApiFeatureSetUsage(true, true, randomCounters());
    }

    @Override
    protected HealthApiFeatureSetUsage mutateInstance(HealthApiFeatureSetUsage instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected Writeable.Reader<HealthApiFeatureSetUsage> instanceReader() {
        return HealthApiFeatureSetUsage::new;
    }

    private Counters randomCounters() {
        if (rarely()) {
            return null;
        }
        Counters counters = new Counters();
        for (int i = 0; i < randomInt(20); i++) {
            if (randomBoolean()) {
                counters.inc(randomAlphaOfLength(10), randomInt(20));
            } else {
                counters.inc(randomAlphaOfLength(10) + "." + randomAlphaOfLength(10), randomInt(20));
            }
        }
        return counters;
    }

    public void testEnrichingValuesEmptyStats() {
        Map<String, Object> noRequestStats = Map.of("invocations", Map.of("total", 0));
        enrichUsageStatsWithValues(noRequestStats);
        assertThat(noRequestStats.keySet(), containsInAnyOrder("invocations"));
    }

    @SuppressWarnings("unchecked")
    public void testEnrichValuesGreenStats() {
        Map<String, Object> greenStats = new HashMap<>();
        int greenCounter = randomInt();
        greenStats.put("statuses", new HashMap<>(Map.of("green", greenCounter)));

        enrichUsageStatsWithValues(greenStats);

        assertThat(greenStats.keySet(), containsInAnyOrder("statuses"));
        Map<String, Object> enriched = (Map<String, Object>) greenStats.get("statuses");
        assertThat(enriched.get("green"), equalTo(greenCounter));
        assertThat((List<String>) enriched.get("values"), containsInAnyOrder("green"));
    }

    @SuppressWarnings("unchecked")
    public void testEnrichingValuesUnhealthyStats() {
        Map<String, Object> stats = new HashMap<>();
        Map<String, Object> statuses = generateStatsMap(
            Arrays.stream(HealthStatus.values()).map(HealthStatus::xContentValue).collect(Collectors.toList())
        );
        stats.put("statuses", statuses);
        List<String> indicatorLabels = List.of("ilm", "slm", "disk", "master_stability", "shards_availability");
        Map<String, Object> redIndicators = generateStatsMap(indicatorLabels);
        Map<String, Object> yellowIndicators = generateStatsMap(indicatorLabels);
        stats.put("indicators", Map.of("red", redIndicators, "yellow", yellowIndicators));
        List<String> diagnosisLabels = IntStream.of(randomIntBetween(1, 50)).mapToObj(i -> "diagnosis:" + i).toList();
        Map<String, Object> redDiagnoses = generateStatsMap(diagnosisLabels);
        Map<String, Object> yellowDiagnoses = generateStatsMap(diagnosisLabels);
        stats.put("diagnoses", Map.of("red", redDiagnoses, "yellow", yellowDiagnoses));

        enrichUsageStatsWithValues(stats);

        assertThat(stats.keySet(), containsInAnyOrder("statuses", "indicators", "diagnoses"));
        assertValues((Map<String, Object>) stats.get("statuses"));
        assertValues(((Map<String, Map<String, Object>>) stats.get("indicators")).get("red"));
        assertValues(((Map<String, Map<String, Object>>) stats.get("indicators")).get("yellow"));
        assertValues(((Map<String, Map<String, Object>>) stats.get("diagnoses")).get("red"));
        assertValues(((Map<String, Map<String, Object>>) stats.get("diagnoses")).get("yellow"));

    }

    @SuppressWarnings("unchecked")
    private static void assertValues(Map<String, Object> map) {
        Set<String> expectedValues = Sets.difference(map.keySet(), Set.of("values"));
        assertThat(map.get("values"), notNullValue());
        assertThat(
            (List<String>) map.get("values"),
            containsInAnyOrder(expectedValues.stream().map(Matchers::equalTo).collect(Collectors.toList()))
        );
    }

    private static Map<String, Object> generateStatsMap(List<String> statLabels) {
        Map<String, Object> statsMap = new HashMap<>();
        for (String stat : randomNonEmptySubsetOf(statLabels)) {
            statsMap.put(stat, randomIntBetween(1, 1_000));
        }
        return statsMap;
    }
}
