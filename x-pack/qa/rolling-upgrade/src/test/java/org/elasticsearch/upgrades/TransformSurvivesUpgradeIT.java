/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.core.IndexerState;
import org.elasticsearch.client.transform.GetTransformStatsResponse;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.TimeSyncConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.TransformStats;
import org.elasticsearch.client.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.test.rest.XPackRestTestConstants.TRANSFORM_INTERNAL_INDEX_PREFIX;
import static org.elasticsearch.xpack.test.rest.XPackRestTestConstants.TRANSFORM_INTERNAL_INDEX_PREFIX_DEPRECATED;
import static org.elasticsearch.xpack.test.rest.XPackRestTestConstants.TRANSFORM_TASK_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.oneOf;

public class TransformSurvivesUpgradeIT extends AbstractUpgradeTestCase {

    private static final String TRANSFORM_ENDPOINT = "/_transform/";
    private static final String CONTINUOUS_TRANSFORM_ID = "continuous-transform-upgrade-job";
    private static final String CONTINUOUS_TRANSFORM_SOURCE = "transform-upgrade-continuous-source";
    private static final List<String> ENTITIES = Stream.iterate(1, n -> n + 1).limit(5).map(v -> "user_" + v).collect(Collectors.toList());
    private static final List<TimeValue> BUCKETS = Stream.iterate(1, n -> n + 1)
        .limit(5)
        .map(TimeValue::timeValueMinutes)
        .collect(Collectors.toList());

    protected static void waitForPendingTransformTasks() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith(TRANSFORM_TASK_NAME) == false);
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setStrictDeprecationMode(false);
        return builder.build();
    }

    /**
     * The purpose of this test is to ensure that when a transform is running through a rolling upgrade it
     * keeps working and does not fail
     */
    public void testTransformRollingUpgrade() throws Exception {
        Request adjustLoggingLevels = new Request("PUT", "/_cluster/settings");
        adjustLoggingLevels.setJsonEntity("""
            {
              "persistent": {
                "logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer": "trace",
                "logger.org.elasticsearch.xpack.dataframe": "trace",
                "logger.org.elasticsearch.xpack.transform": "trace"
              }
            }""");
        client().performRequest(adjustLoggingLevels);
        Request waitForYellow = new Request("GET", "/_cluster/health");
        waitForYellow.addParameter("wait_for_nodes", "3");
        waitForYellow.addParameter("wait_for_status", "yellow");
        switch (CLUSTER_TYPE) {
            case OLD:
                client().performRequest(waitForYellow);
                createAndStartContinuousTransform();
                break;
            case MIXED:
                client().performRequest(waitForYellow);
                long lastCheckpoint = 1;
                if (Booleans.parseBoolean(System.getProperty("tests.first_round")) == false) {
                    lastCheckpoint = 2;
                }
                verifyContinuousTransformHandlesData(lastCheckpoint);
                verifyUpgradeFailsIfMixedCluster();
                break;
            case UPGRADED:
                client().performRequest(waitForYellow);
                verifyContinuousTransformHandlesData(3);
                verifyUpgrade();
                cleanUpTransforms();
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void cleanUpTransforms() throws Exception {
        stopTransform(CONTINUOUS_TRANSFORM_ID);
        deleteTransform(CONTINUOUS_TRANSFORM_ID);
        waitForPendingTransformTasks();
    }

    private void createAndStartContinuousTransform() throws Exception {
        createIndex(CONTINUOUS_TRANSFORM_SOURCE);
        long totalDocsWrittenSum = 0;
        for (TimeValue bucket : BUCKETS) {
            int docs = randomIntBetween(1, 25);
            putData(CONTINUOUS_TRANSFORM_SOURCE, docs, bucket, ENTITIES);
            totalDocsWrittenSum += docs * ENTITIES.size();
        }
        long totalDocsWritten = totalDocsWrittenSum;
        TransformConfig config = TransformConfig.builder()
            .setSyncConfig(TimeSyncConfig.builder().setField("timestamp").setDelay(TimeValue.timeValueSeconds(1)).build())
            .setPivotConfig(
                PivotConfig.builder()
                    .setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("stars").field("stars")))
                    .setGroups(GroupConfig.builder().groupBy("user_id", TermsGroupSource.builder().setField("user_id").build()).build())
                    .build()
            )
            .setDest(DestConfig.builder().setIndex(CONTINUOUS_TRANSFORM_ID + "_idx").build())
            .setSource(SourceConfig.builder().setIndex(CONTINUOUS_TRANSFORM_SOURCE).build())
            .setId(CONTINUOUS_TRANSFORM_ID)
            .setFrequency(TimeValue.timeValueSeconds(1))
            .build();
        putTransform(CONTINUOUS_TRANSFORM_ID, config);

        startTransform(CONTINUOUS_TRANSFORM_ID);
        waitUntilAfterCheckpoint(CONTINUOUS_TRANSFORM_ID, 0L);

        assertBusy(() -> {
            TransformStats stateAndStats = getTransformStats(CONTINUOUS_TRANSFORM_ID);
            assertThat(stateAndStats.getIndexerStats().getDocumentsIndexed(), equalTo((long) ENTITIES.size()));
            assertThat(stateAndStats.getIndexerStats().getDocumentsProcessed(), equalTo(totalDocsWritten));
            // Even if we get back to started, we may periodically get set back to `indexing` when triggered.
            // Though short lived due to no changes on the source indices, it could result in flaky test behavior
            assertThat(stateAndStats.getState(), oneOf(TransformStats.State.STARTED, TransformStats.State.INDEXING));
        }, 120, TimeUnit.SECONDS);

        // We want to make sure our latest state is written before we turn the node off, this makes the testing more reliable
        awaitWrittenIndexerState(CONTINUOUS_TRANSFORM_ID, IndexerState.STARTED.value());
    }

    @SuppressWarnings("unchecked")
    private void verifyContinuousTransformHandlesData(long expectedLastCheckpoint) throws Exception {

        // A continuous transform should automatically become started when it gets assigned to a node
        // if it was assigned to the node that was removed from the cluster
        assertBusy(() -> {
            TransformStats stateAndStats = getTransformStats(CONTINUOUS_TRANSFORM_ID);
            assertThat(stateAndStats.getState(), oneOf(TransformStats.State.STARTED, TransformStats.State.INDEXING));
        }, 120, TimeUnit.SECONDS);

        TransformStats previousStateAndStats = getTransformStats(CONTINUOUS_TRANSFORM_ID);

        // Add a new user and write data to it
        // This is so we can have more reliable data counts, as writing to existing entities requires
        // rescanning the past data
        List<String> entities = new ArrayList<>(1);
        entities.add("user_" + ENTITIES.size() + expectedLastCheckpoint);
        int docs = 5;
        // Index the data
        // The frequency and delay should see the data once its indexed
        putData(CONTINUOUS_TRANSFORM_SOURCE, docs, TimeValue.timeValueSeconds(0), entities);

        waitUntilAfterCheckpoint(CONTINUOUS_TRANSFORM_ID, expectedLastCheckpoint);

        assertBusy(
            () -> assertThat(
                getTransformStats(CONTINUOUS_TRANSFORM_ID).getIndexerStats().getDocumentsProcessed(),
                greaterThanOrEqualTo(docs + previousStateAndStats.getIndexerStats().getDocumentsProcessed())
            ),
            120,
            TimeUnit.SECONDS
        );
        TransformStats stateAndStats = getTransformStats(CONTINUOUS_TRANSFORM_ID);

        assertThat(stateAndStats.getState(), oneOf(TransformStats.State.STARTED, TransformStats.State.INDEXING));
        awaitWrittenIndexerState(CONTINUOUS_TRANSFORM_ID, (responseBody) -> {
            Map<String, Object> indexerStats = (Map<String, Object>) ((List<?>) XContentMapValues.extractValue(
                "hits.hits._source.stats",
                responseBody
            )).get(0);
            assertThat(
                (Integer) indexerStats.get("documents_indexed"),
                greaterThan(Long.valueOf(previousStateAndStats.getIndexerStats().getDocumentsIndexed()).intValue())
            );
            assertThat(
                (Integer) indexerStats.get("documents_processed"),
                greaterThan(Long.valueOf(previousStateAndStats.getIndexerStats().getDocumentsProcessed()).intValue())
            );
        });
    }

    private void verifyUpgradeFailsIfMixedCluster() {
        // upgrade tests by design are also executed with the same version, this check must be skipped in this case, see gh#39102.
        if (UPGRADE_FROM_VERSION.equals(Version.CURRENT)) {
            return;
        }
        final Request upgradeTransformRequest = new Request("POST", getTransformEndpoint() + "_upgrade");

        Exception ex = expectThrows(Exception.class, () -> client().performRequest(upgradeTransformRequest));
        assertThat(ex.getMessage(), containsString("All nodes must be the same version"));
    }

    private void verifyUpgrade() throws IOException {
        final Request upgradeTransformRequest = new Request("POST", getTransformEndpoint() + "_upgrade");
        Response response = client().performRequest(upgradeTransformRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void awaitWrittenIndexerState(String id, Consumer<Map<?, ?>> responseAssertion) throws Exception {
        Request getStatsDocsRequest = new Request(
            "GET",
            TRANSFORM_INTERNAL_INDEX_PREFIX + "*," + TRANSFORM_INTERNAL_INDEX_PREFIX_DEPRECATED + "*" + "/_search"
        );

        getStatsDocsRequest.setJsonEntity("""
            {
               "query": {
                 "bool": {
                   "filter": {
                     "term": {
                       "_id": "data_frame_transform_state_and_stats-%s"
                     }
                   }
                 }
               },
               "sort": [ { "_index": { "order": "desc" } } ],
               "size": 1
             }""".formatted(id));
        assertBusy(() -> {
            // Want to make sure we get the latest docs
            client().performRequest(new Request("POST", TRANSFORM_INTERNAL_INDEX_PREFIX + "*/_refresh"));
            client().performRequest(new Request("POST", TRANSFORM_INTERNAL_INDEX_PREFIX_DEPRECATED + "*/_refresh"));
            Response response = client().performRequest(getStatsDocsRequest);
            assertEquals(200, response.getStatusLine().getStatusCode());
            Map<String, Object> responseBody = entityAsMap(response);
            assertEquals("expected only 1 hit, got: " + responseBody, 1, XContentMapValues.extractValue("hits.total.value", responseBody));
            responseAssertion.accept(responseBody);
        }, 60, TimeUnit.SECONDS);
    }

    private void awaitWrittenIndexerState(String id, String indexerState) throws Exception {
        awaitWrittenIndexerState(id, (responseBody) -> {
            String storedState = ((List<?>) XContentMapValues.extractValue("hits.hits._source.state.indexer_state", responseBody)).get(0)
                .toString();
            assertThat(storedState, equalTo(indexerState));
        });
    }

    private String getTransformEndpoint() {
        return TRANSFORM_ENDPOINT;
    }

    private void putTransform(String id, TransformConfig config) throws IOException {
        final Request createDataframeTransformRequest = new Request("PUT", getTransformEndpoint() + id);
        createDataframeTransformRequest.setJsonEntity(Strings.toString(config));
        Response response = client().performRequest(createDataframeTransformRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void deleteTransform(String id) throws IOException {
        Response response = client().performRequest(new Request("DELETE", getTransformEndpoint() + id));
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void startTransform(String id) throws IOException {
        final Request startDataframeTransformRequest = new Request("POST", getTransformEndpoint() + id + "/_start");
        Response response = client().performRequest(startDataframeTransformRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void stopTransform(String id) throws IOException {
        final Request stopDataframeTransformRequest = new Request("POST", getTransformEndpoint() + id + "/_stop?wait_for_completion=true");
        Response response = client().performRequest(stopDataframeTransformRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private TransformStats getTransformStats(String id) throws IOException {
        final Request getStats = new Request("GET", getTransformEndpoint() + id + "/_stats");
        Response response = client().performRequest(getStats);
        assertEquals(200, response.getStatusLine().getStatusCode());
        XContentType xContentType = XContentType.fromMediaType(response.getEntity().getContentType().getValue());
        try (
            XContentParser parser = xContentType.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
        ) {
            GetTransformStatsResponse resp = GetTransformStatsResponse.fromXContent(parser);
            assertThat(resp.getTransformsStats(), hasSize(1));
            return resp.getTransformsStats().get(0);
        }
    }

    private void waitUntilAfterCheckpoint(String id, long currentCheckpoint) throws Exception {
        assertBusy(
            () -> assertThat(getTransformStats(id).getCheckpointingInfo().getLast().getCheckpoint(), greaterThan(currentCheckpoint)),
            60,
            TimeUnit.SECONDS
        );
    }

    private void createIndex(String indexName) throws IOException {
        // create mapping
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("mappings")
                    .startObject("properties")
                    .startObject("timestamp")
                    .field("type", "date")
                    .endObject()
                    .startObject("user_id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("stars")
                    .field("type", "integer")
                    .endObject()
                    .endObject()
                    .endObject();
            }
            builder.endObject();
            final StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
            Request req = new Request("PUT", indexName);
            req.setEntity(entity);
            assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
        }
    }

    private void putData(String indexName, int numDocs, TimeValue fromTime, List<String> entityIds) throws IOException {
        long timeStamp = Instant.now().toEpochMilli() - fromTime.getMillis();

        // create index
        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            for (String entity : entityIds) {
                bulk.append("""
                    {"index":{"_index":"%s"}}
                    {"user_id":"%s","stars":%s,"timestamp":%s}
                    """.formatted(indexName, entity, randomLongBetween(0, 5), timeStamp));
            }
        }
        bulk.append("\r\n");
        final Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        entityAsMap(client().performRequest(bulkRequest));
    }
}
