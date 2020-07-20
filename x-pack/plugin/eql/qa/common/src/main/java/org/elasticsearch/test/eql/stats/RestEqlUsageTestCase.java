/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.test.eql.stats;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.eql.DataLoader;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.eql.DataLoader.testIndexName;

/**
 * Tests a random number of queries that increase various (most of the times, one query will "touch" multiple metrics values) metrics. 
 */
public abstract class RestEqlUsageTestCase extends ESRestTestCase {

    private RestHighLevelClient highLevelClient;
    private Map<String, Integer> baseMetrics = new HashMap<String, Integer>();
    private Integer baseAllTotalQueries = 0;
    private Integer baseAllFailedQueries = 0;

    @BeforeClass
    public static void checkForSnapshot() {
        assumeTrue("Only works on snapshot builds for now", Build.CURRENT.isSnapshot());
    }

    /**
     * This method gets the metrics' values before the test runs, in case these values
     * were changed by other tests running in the same REST test cluster. The test itself
     * will count the new metrics' values starting from the base values initialized here.
     * These values will increase during the execution of the test with updates in {@link #assertFeatureMetric(int, Map, String)}
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Before
    private void getBaseMetrics() throws UnsupportedOperationException, IOException {
        Map<String, Object> baseStats = getStats();
        List<Map<String, Map<String, Map>>> nodesListStats = (List) baseStats.get("stats");

        for (Map perNodeStats : nodesListStats) {
            Map queriesMetrics = (Map) ((Map) perNodeStats.get("stats")).get("queries"); 
            Map featuresMetrics = getFeaturesMetrics(perNodeStats);

            for (FeatureMetric metric : FeatureMetric.values()) {
                String metricName = metric.toString();
                if (baseMetrics.containsKey(metricName)) {
                    baseMetrics.put(metricName, baseMetrics.get(metricName) + ((Integer) featuresMetrics.get(metricName)));
                } else {
                    baseMetrics.put(metricName, (Integer) featuresMetrics.get(metricName));
                }
            }

            // initialize the "base" metric values with whatever values are already recorded on ES
            baseAllTotalQueries += ((Map<String, Integer>) queriesMetrics.get("_all")).get("total");
            baseAllFailedQueries += ((Map<String, Integer>) queriesMetrics.get("_all")).get("failed");
        }
    }

    /**
     * "Flatten" the response from ES putting all the features metrics in the same Map.
     *          "features": {
     *              "joins": {
     *                  "join_queries_three": 0,
     *                  "join_queries_two": 0,
     *                  "join_until": 0,
     *                  "join_queries_five_or_more": 0,
     *                  "join_queries_four": 0
     *              },
     *              "sequence": 0,
     *              "keys": {
     *                  "join_keys_two": 0,
     *                  "join_keys_one": 0,
     *                  "join_keys_three": 0,
     *                  "join_keys_five_or_more": 0,
     *                  "join_keys_four": 0
     *              },
     *              "join": 0,
     *              "sequences": {
     *                  "sequence_queries_three": 0,
     *                  "sequence_queries_four": 0,
     *                  "sequence_queries_two": 0,
     *                  "sequence_until": 0,
     *                  "sequence_queries_five_or_more": 0,
     *                  "sequence_maxspan": 0
     *              },
     *              "event": 0,
     *              "pipes": {
     *                  "pipe_tail": 0,
     *                  "pipe_head": 0
     *              }
     *          }
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Map getFeaturesMetrics(Map perNodeStats) {
        Map featuresMetrics = (Map) ((Map) perNodeStats.get("stats")).get("features");
        featuresMetrics.putAll((Map) featuresMetrics.get("keys"));
        featuresMetrics.putAll((Map) featuresMetrics.get("sequences"));
        featuresMetrics.putAll((Map) featuresMetrics.get("joins"));
        featuresMetrics.putAll((Map) featuresMetrics.get("pipes"));
        return featuresMetrics;
    }

    public void testEqlRestUsage() throws IOException {
        // create the index and load the data, if the index doesn't exist
        // it doesn't matter if the index is already there (probably created by another test); _if_ its mapping is the expected one 
        // it should be enough
        if (client().performRequest(new Request("HEAD", "/" + testIndexName)).getStatusLine().getStatusCode() == 404) {
            DataLoader.loadDatasetIntoEs(highLevelClient(), (t, u) -> createParser(t, u));
        }

        //
        // random event queries
        //
        int randomEventExecutions = randomIntBetween(1, 15);
        int allTotalQueries = baseAllTotalQueries + randomEventExecutions;

        for (int i = 0; i < randomEventExecutions; i++) {
            runEql("process where serial_event_id < 4 | head 3");
        }

        Map<String, Object> responseAsMap = getStats();
        Set<String> metricsToCheck = Set.of("pipe_head", "event");
        assertFeaturesMetrics(randomEventExecutions, responseAsMap, metricsToCheck);
        assertFeaturesMetricsExcept(responseAsMap, metricsToCheck);
        assertAllQueryMetrics(allTotalQueries, responseAsMap);

        //
        // random two sequences queries
        //
        int randomSequenceExecutions = randomIntBetween(1, 15);
        allTotalQueries += randomSequenceExecutions;
        for (int i = 0; i < randomSequenceExecutions; i++) {
            runEql("sequence [process where serial_event_id = 1] [process where serial_event_id = 2]");
        }
        responseAsMap = getStats();
        metricsToCheck = Set.of("sequence", "sequence_queries_two", "pipe_head");
        assertFeaturesMetrics(randomSequenceExecutions, responseAsMap, metricsToCheck);
        assertFeaturesMetricsExcept(responseAsMap, metricsToCheck);
        assertAllQueryMetrics(allTotalQueries, responseAsMap);

        //
        // random tail queries
        //
        int randomTailExecutions = randomIntBetween(1, 15);
        allTotalQueries += randomTailExecutions;
        for (int i = 0; i < randomTailExecutions; i++) {
            runEql("process where serial_event_id < 4 | tail 2");
        }
        responseAsMap = getStats();
        metricsToCheck = Set.of("pipe_tail", "event");
        assertFeaturesMetrics(randomTailExecutions, responseAsMap, metricsToCheck);
        assertFeaturesMetricsExcept(responseAsMap, metricsToCheck);
        assertAllQueryMetrics(allTotalQueries, responseAsMap);

        //
        // random sequence with maxspan and four queries
        //
        int randomMaxspanExecutions = randomIntBetween(1, 15);
        allTotalQueries += randomMaxspanExecutions;
        for (int i = 0; i < randomMaxspanExecutions; i++) {
            runEql("sequence with maxspan=1d" + 
                "  [process where serial_event_id < 4] by exit_code" + 
                "  [process where opcode == 1] by user" + 
                "  [process where opcode == 2] by user" + 
                "  [file where parent_process_name == 'file_delete_event'] by exit_code" +
                " until [process where opcode=1] by ppid" + 
                " | head 4" + 
                " | tail 2");
        }
        responseAsMap = getStats();
        metricsToCheck = Set.of("sequence", "sequence_maxspan", "sequence_queries_four", "pipe_head", "pipe_tail", "join_keys_one",
            "sequence_until");
        assertFeaturesMetrics(randomMaxspanExecutions, responseAsMap, metricsToCheck);
        assertFeaturesMetricsExcept(responseAsMap, metricsToCheck);
        assertAllQueryMetrics(allTotalQueries, responseAsMap);

        //
        // random sequence with three queries
        //
        int randomThreeQueriesSequences = randomIntBetween(1, 15);
        allTotalQueries += randomThreeQueriesSequences;
        for (int i = 0; i < randomThreeQueriesSequences; i++) {
            runEql("sequence with maxspan=1d" + 
                "  [process where serial_event_id < 4] by exit_code" + 
                "  [process where opcode == 1] by user" + 
                "  [process where opcode == 2] by user");
        }
        responseAsMap = getStats();
        metricsToCheck = Set.of("sequence", "sequence_queries_three", "pipe_head", "join_keys_one", "sequence_maxspan");
        assertFeaturesMetrics(randomThreeQueriesSequences, responseAsMap, metricsToCheck);
        assertFeaturesMetricsExcept(responseAsMap, metricsToCheck);
        assertAllQueryMetrics(allTotalQueries, responseAsMap);

        //
        // random sequence with five queries and three join keys
        //
        int randomFiveQueriesSequences = randomIntBetween(1, 15);
        allTotalQueries += randomFiveQueriesSequences;
        for (int i = 0; i < randomFiveQueriesSequences; i++) {
            runEql("sequence by user, ppid, exit_code with maxspan=1m" + 
                "  [process where serial_event_id < 4]" + 
                "  [process where opcode == 1]" + 
                "  [file where parent_process_name == 'file_delete_event']" +
                "  [process where serial_event_id < 4]" + 
                "  [process where opcode == 1]" + 
                "| tail 4");
        }
        responseAsMap = getStats();
        metricsToCheck = Set.of("sequence", "sequence_queries_five_or_more", "pipe_tail", "join_keys_three", "sequence_maxspan");
        assertFeaturesMetrics(randomFiveQueriesSequences, responseAsMap, metricsToCheck);
        assertFeaturesMetricsExcept(responseAsMap, metricsToCheck);
        assertAllQueryMetrics(allTotalQueries, responseAsMap);

        //
        // random sequence with four join keys
        //
        int randomFourJoinKeysExecutions = randomIntBetween(1, 15);
        allTotalQueries += randomFourJoinKeysExecutions;
        for (int i = 0; i < randomFourJoinKeysExecutions; i++) {
            runEql("sequence by exit_code, user, serial_event_id, pid" + 
                "  [process where serial_event_id < 4]" + 
                "  [process where opcode == 1]");
        }
        responseAsMap = getStats();
        metricsToCheck = Set.of("sequence", "sequence_queries_two", "pipe_head", "join_keys_four");
        assertFeaturesMetrics(randomFourJoinKeysExecutions, responseAsMap, metricsToCheck);
        assertFeaturesMetricsExcept(responseAsMap, metricsToCheck);
        assertAllQueryMetrics(allTotalQueries, responseAsMap);

        //
        // random sequence with five join keys
        //
        int randomFiveJoinKeysExecutions = randomIntBetween(1, 15);
        allTotalQueries += randomFiveJoinKeysExecutions;
        for (int i = 0; i < randomFiveJoinKeysExecutions; i++) {
            runEql("sequence by exit_code, user, serial_event_id, pid, ppid" + 
                "  [process where serial_event_id < 4]" + 
                "  [process where opcode == 1]");
        }
        responseAsMap = getStats();
        metricsToCheck = Set.of("sequence", "sequence_queries_two", "pipe_head", "join_keys_five_or_more");
        assertFeaturesMetrics(randomFiveJoinKeysExecutions, responseAsMap, metricsToCheck);
        assertFeaturesMetricsExcept(responseAsMap, metricsToCheck);
        assertAllQueryMetrics(allTotalQueries, responseAsMap);

        //
        // random failed queries
        //
        int randomFailedExecutions = randomIntBetween(1, 15);
        int allFailedQueries = baseAllFailedQueries + randomFailedExecutions;
        allTotalQueries += randomFailedExecutions;
        for (int i = 0; i < randomFailedExecutions; i++) {
            // not interested in the exception type, but in the fact that the metrics are incremented when an exception is thrown
            expectThrows(Exception.class, () -> {
                runEql(
                    randomFrom(
                        "process where missing_field < 4 | tail 2",
                        "sequence abc [process where serial_event_id = 1]",
                        "sequence with maxspan=1x [process where serial_event_id = 1]",
                        "sequence by exit_code, user [process where serial_event_id < 4] by ppid",
                        "sequence by"
                    )
                );
            });
        }
        responseAsMap = getStats();
        assertAllFailedQueryMetrics(allFailedQueries, responseAsMap);
        assertAllQueryMetrics(allTotalQueries, responseAsMap);
    }

    private void assertAllQueryMetrics(int allTotalQueries, Map<String, Object> responseAsMap) throws IOException {
        assertAllQueryMetric(allTotalQueries, responseAsMap, "total");
    }

    private void assertAllFailedQueryMetrics(int allFailedQueries, Map<String, Object> responseAsMap) throws IOException {
        assertAllQueryMetric(allFailedQueries, responseAsMap, "failed");
    }

    private Map<String, Object> getStats() throws UnsupportedOperationException, IOException {
        Request request = new Request("GET", "/_eql/stats");
        Map<String, Object> responseAsMap;
        try (InputStream content = client().performRequest(request).getEntity().getContent()) {
            responseAsMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }

        return responseAsMap;
    }

    private void runEql(String eql) throws IOException {
        Request request = new Request("POST", DataLoader.testIndexName + "/_eql/search");
        request.setJsonEntity("{\"query\":\"" + eql +"\"}");
        client().performRequest(request);
    }

    private void assertFeaturesMetrics(int expected, Map<String, Object> responseAsMap, Set<String> metricsToCheck) throws IOException {
        for(String metricName : metricsToCheck) {
            assertFeatureMetric(expected, responseAsMap, metricName);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void assertFeatureMetric(int expected, Map<String, Object> responseAsMap, String feature) throws IOException {
        List<Map<String, ?>> nodesListStats = (List<Map<String, ?>>) responseAsMap.get("stats");
        int actualMetricValue = 0;
        for (Map perNodeStats : nodesListStats) {
            Map featuresMetrics = getFeaturesMetrics(perNodeStats);
            actualMetricValue += (int) featuresMetrics.get(feature);
        }
        assertEquals(expected + baseMetrics.get(feature), actualMetricValue);

        /*
         * update the base value for future checks in {@link #assertFeaturesMetricsExcept(Set, Map)}
         */
        baseMetrics.put(feature, expected + baseMetrics.get(feature));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void assertQueryMetric(int expected, Map<String, Object> responseAsMap, String queryType, String metric) throws IOException {
        List<Map<String, Map<String, Map>>> nodesListStats = (List) responseAsMap.get("stats");
        int actualMetricValue = 0;
        for (Map perNodeStats : nodesListStats) {
            Map queriesMetrics = (Map) ((Map) perNodeStats.get("stats")).get("queries");
            Map perTypeQueriesMetrics = (Map) queriesMetrics.get(queryType);
            actualMetricValue += (int) perTypeQueriesMetrics.get(metric);
        }
        assertEquals(expected, actualMetricValue);
    }

    private void assertAllQueryMetric(int expected, Map<String, Object> responseAsMap, String metric) throws IOException {
        assertQueryMetric(expected, responseAsMap, "_all", metric);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void assertFeaturesMetricsExcept(Map<String, Object> responseAsMap, Set<String> exceptions) {
        List<Map<String, ?>> nodesListStats = (List<Map<String, ?>>) responseAsMap.get("stats");
        for (FeatureMetric metric : FeatureMetric.values()) {
            String metricName = metric.toString();
            if (exceptions.contains(metricName) == false) {
                Integer actualValue = 0;
                for (Map perNodeStats : nodesListStats) {
                    Map featuresMetrics = getFeaturesMetrics(perNodeStats);
                    Integer featureMetricValue = (Integer) featuresMetrics.get(metricName);
                    actualValue += featureMetricValue;
                }

                assertEquals(baseMetrics.get(metricName), actualValue);
            }
        }
    }

    private RestHighLevelClient highLevelClient() {
        if (highLevelClient == null) {
            highLevelClient = new RestHighLevelClient(
                    client(),
                    ignore -> {
                    },
                    Collections.emptyList()) {
            };
        }
        return highLevelClient;
    }
}
