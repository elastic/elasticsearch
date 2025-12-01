/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntFunction;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

/**
 * Tests that run ESQL lookup join and enrich queries that use a ton of memory.
 * We want to make sure they don't consume the entire heap and crash Elasticsearch.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackLookupJoinIT extends HeapAttackTestCase {

    public void testLookupExplosion() throws IOException {
        int sensorDataCount = 400;
        int lookupEntries = 10000;
        Map<?, ?> map = lookupExplosion(sensorDataCount, lookupEntries, 1, lookupEntries, false);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount * lookupEntries))));
    }

    public void testLookupExplosionManyFields() throws IOException {
        int sensorDataCount = 400;
        int lookupEntries = 1000;
        int joinFieldsCount = 990;
        Map<?, ?> map = lookupExplosion(sensorDataCount, lookupEntries, joinFieldsCount, lookupEntries, false);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount * lookupEntries))));
    }

    public void testLookupExplosionExpression() throws IOException {
        int sensorDataCount = 400;
        int lookupEntries = 10000;
        Map<?, ?> map = lookupExplosion(sensorDataCount, lookupEntries, 1, lookupEntries, true);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount * lookupEntries))));
    }

    public void testLookupExplosionManyFieldsExpression() throws IOException {
        int sensorDataCount = 400;
        int lookupEntries = 1000;
        int joinFieldsCount = 399;// only join on 399 columns due to max expression size of 400
        Map<?, ?> map = lookupExplosion(sensorDataCount, lookupEntries, joinFieldsCount, lookupEntries, true);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount * lookupEntries))));
    }

    public void testLookupExplosionManyMatchesManyFields() throws IOException {
        // 1500, 10000 is enough locally, but some CI machines need more.
        int lookupEntries = 10000;
        assertCircuitBreaks(attempt -> lookupExplosion(attempt * 1500, lookupEntries, 30, lookupEntries, false));
    }

    public void testLookupExplosionManyMatches() throws IOException {
        // 1500, 10000 is enough locally, but some CI machines need more.
        int lookupEntries = 10000;
        assertCircuitBreaks(attempt -> lookupExplosion(attempt * 1500, lookupEntries, 1, lookupEntries, false));
    }

    public void testLookupExplosionManyMatchesExpression() throws IOException {
        int lookupEntries = 10000;
        assertCircuitBreaks(attempt -> lookupExplosion(attempt * 1500, lookupEntries, 1, lookupEntries, true));
    }

    public void testLookupExplosionManyMatchesFiltered() throws IOException {
        // This test will only work with the expanding join optimization
        // that pushes the filter to the right side of the lookup.
        // Without the optimization, it will fail with circuit_breaking_exception
        int sensorDataCount = 10000;
        int lookupEntries = 10000;
        int reductionFactor = 1000; // reduce the number of matches by this factor
        // lookupEntries % reductionFactor must be 0 to ensure the number of rows returned matches the expected value
        assertTrue(0 == lookupEntries % reductionFactor);
        Map<?, ?> map = lookupExplosion(sensorDataCount, lookupEntries, 1, lookupEntries / reductionFactor, false);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount * lookupEntries / reductionFactor))));
    }

    public void testLookupExplosionManyMatchesFilteredExpression() throws IOException {
        // This test will only work with the expanding join optimization
        // that pushes the filter to the right side of the lookup.
        // Without the optimization, it will fail with circuit_breaking_exception
        int sensorDataCount = 10000;
        int lookupEntries = 10000;
        int reductionFactor = 1000; // reduce the number of matches by this factor
        // lookupEntries % reductionFactor must be 0 to ensure the number of rows returned matches the expected value
        assertTrue(0 == lookupEntries % reductionFactor);
        Map<?, ?> map = lookupExplosion(sensorDataCount, lookupEntries, 1, lookupEntries / reductionFactor, true);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount * lookupEntries / reductionFactor))));
    }

    public void testLookupExplosionNoFetch() throws IOException {
        int sensorDataCount = 6000;
        int lookupEntries = 10000;
        Map<?, ?> map = lookupExplosionNoFetch(sensorDataCount, lookupEntries);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount * lookupEntries))));
    }

    public void testLookupExplosionNoFetchManyMatches() throws IOException {
        // 8500 is plenty on most nodes
        assertCircuitBreaks(attempt -> lookupExplosionNoFetch(attempt * 8500, 10000));
    }

    public void testLookupExplosionBigString() throws IOException {
        int sensorDataCount = 200;
        int lookupEntries = 1;
        Map<?, ?> map = lookupExplosionBigString(sensorDataCount, lookupEntries);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount * lookupEntries))));
    }

    public void testLookupExplosionBigStringManyMatches() throws IOException {
        // 500, 1 is enough with a single node, but the serverless copy of this test uses many nodes.
        // So something like 5000, 10 is much more of a sure thing there.
        assertCircuitBreaks(attempt -> lookupExplosionBigString(attempt * 5000, 10));
    }

    private Map<String, Object> lookupExplosion(
        int sensorDataCount,
        int lookupEntries,
        int joinFieldsCount,
        int lookupEntriesToKeep,
        boolean expressionBasedJoin
    ) throws IOException {
        try {
            lookupExplosionData(sensorDataCount, lookupEntries, joinFieldsCount, expressionBasedJoin);
            StringBuilder query = startQuery();
            query.append("FROM sensor_data | LOOKUP JOIN sensor_lookup ON ");
            if (expressionBasedJoin) {
                for (int i = 0; i < joinFieldsCount; i++) {
                    if (i != 0) {
                        query.append(" AND ");
                    }
                    query.append("id_left").append(i);
                    query.append("==");
                    query.append("id_right").append(i);
                }
            } else {
                for (int i = 0; i < joinFieldsCount; i++) {
                    if (i != 0) {
                        query.append(",");
                    }
                    query.append("id").append(i);
                }
            }
            if (lookupEntries != lookupEntriesToKeep) {
                boolean applyAsExpressionJoinFilter = expressionBasedJoin && randomBoolean();
                // we randomly add the filter after the join or as part of the join
                // in both cases we should have the same amount of results
                if (applyAsExpressionJoinFilter == false) {
                    // add a filter after the join to reduce the number of matches
                    // we add both a Lucene pushable filter and a non-pushable filter
                    // this is to make sure that even if there are non-pushable filters the pushable filters is still applied
                    query.append(" | WHERE ABS(filter_key) > -1 AND filter_key < ").append(lookupEntriesToKeep);
                } else {
                    // apply the filter as part of the join
                    // then we filter out the rows that do not match the filter after
                    // so the number of rows is the same as in the field based join case
                    // and can get the same number of rows for verification purposes
                    query.append(" AND filter_key < ").append(lookupEntriesToKeep);
                    query.append(" | WHERE filter_key IS NOT NULL ");
                }
            }
            query.append(" | STATS COUNT(location) | LIMIT 100\"}");
            return responseAsMap(query(query.toString(), null));
        } finally {
            deleteIndex("sensor_data");
            deleteIndex("sensor_lookup");
        }
    }

    private Map<String, Object> lookupExplosionNoFetch(int sensorDataCount, int lookupEntries) throws IOException {
        try {
            lookupExplosionData(sensorDataCount, lookupEntries, 1, false);
            StringBuilder query = startQuery();
            query.append("FROM sensor_data | LOOKUP JOIN sensor_lookup ON id0 | STATS COUNT(*)\"}");
            return responseAsMap(query(query.toString(), null));
        } finally {
            deleteIndex("sensor_data");
            deleteIndex("sensor_lookup");
        }
    }

    private void lookupExplosionData(int sensorDataCount, int lookupEntries, int joinFieldCount, boolean expressionBasedJoin)
        throws IOException {
        initSensorData(sensorDataCount, 1, joinFieldCount, expressionBasedJoin);
        initSensorLookup(lookupEntries, 1, i -> "73.9857 40.7484", joinFieldCount, expressionBasedJoin);
    }

    private Map<String, Object> lookupExplosionBigString(int sensorDataCount, int lookupEntries) throws IOException {
        try {
            initSensorData(sensorDataCount, 1, 1, false);
            initSensorLookupString(lookupEntries, 1, i -> {
                int target = Math.toIntExact(ByteSizeValue.ofMb(1).getBytes());
                StringBuilder str = new StringBuilder(Math.toIntExact(ByteSizeValue.ofMb(2).getBytes()));
                while (str.length() < target) {
                    str.append("Lorem ipsum dolor sit amet, consectetur adipiscing elit.");
                }
                logger.info("big string is {} characters", str.length());
                return str.toString();
            });
            StringBuilder query = startQuery();
            query.append("FROM sensor_data | LOOKUP JOIN sensor_lookup ON id0 | STATS COUNT(string)\"}");
            return responseAsMap(query(query.toString(), null));
        } finally {
            deleteIndex("sensor_data");
            deleteIndex("sensor_lookup");
        }
    }

    private void initSensorLookupString(int lookupEntries, int sensorCount, IntFunction<String> string) throws IOException {
        logger.info("loading sensor lookup with huge strings");
        createIndex("sensor_lookup", Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP.getName()).build(), """
            {
                "properties": {
                    "id0": { "type": "long" },
                    "string": { "type": "text" }
                }
            }""");
        int docsPerBulk = 10;
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < lookupEntries; i++) {
            int sensor = i % sensorCount;
            data.append(String.format(Locale.ROOT, """
                {"create":{}}
                {"id0": %d, "string": "%s"}
                """, sensor, string.apply(sensor)));
            if (i % docsPerBulk == docsPerBulk - 1) {
                bulk("sensor_lookup", data.toString());
                data.setLength(0);
            }
        }
        initIndex("sensor_lookup", data.toString());
    }

    public void testEnrichExplosion() throws IOException {
        int sensorDataCount = 1000;
        int lookupEntries = 100;
        Map<?, ?> map = enrichExplosion(sensorDataCount, lookupEntries);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount))));
    }

    public void testEnrichExplosionManyMatches() throws IOException {
        // 1000, 10000 is enough on most nodes
        assertCircuitBreaks(attempt -> enrichExplosion(1000, attempt * 5000));
    }

    private Map<String, Object> enrichExplosion(int sensorDataCount, int lookupEntries) throws IOException {
        try {
            initSensorData(sensorDataCount, 1, 1, false);
            initSensorEnrich(lookupEntries, 1, i -> "73.9857 40.7484");
            try {
                StringBuilder query = startQuery();
                query.append("FROM sensor_data | ENRICH sensor ON id0 | STATS COUNT(*)\"}");
                return responseAsMap(query(query.toString(), null));
            } finally {
                Request delete = new Request("DELETE", "/_enrich/policy/sensor");
                assertMap(responseAsMap(client().performRequest(delete)), matchesMap().entry("acknowledged", true));
            }
        } finally {
            deleteIndex("sensor_data");
            deleteIndex("sensor_lookup");
        }
    }

    private void initSensorEnrich(int lookupEntries, int sensorCount, IntFunction<String> location) throws IOException {
        initSensorLookup(lookupEntries, sensorCount, location, 1, false);
        logger.info("loading sensor enrich");

        Request create = new Request("PUT", "/_enrich/policy/sensor");
        create.setJsonEntity("""
            {
              "match": {
                "indices": "sensor_lookup",
                "match_field": "id0",
                "enrich_fields": ["location"]
              }
            }
            """);
        assertMap(responseAsMap(client().performRequest(create)), matchesMap().entry("acknowledged", true));
        Request execute = new Request("POST", "/_enrich/policy/sensor/_execute");
        assertMap(responseAsMap(client().performRequest(execute)), matchesMap().entry("status", Map.of("phase", "COMPLETE")));
    }

}
