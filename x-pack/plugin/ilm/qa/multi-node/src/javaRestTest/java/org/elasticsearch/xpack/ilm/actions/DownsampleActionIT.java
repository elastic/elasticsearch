/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.actions;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ilm.CheckNotDataStreamWriteIndexStep;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createIndexWithSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getOnlyIndexSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.index;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.rolloverMaxOneDocCondition;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.updatePolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DownsampleActionIT extends ESRestTestCase {

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);
    private String index;
    private String policy;
    private String alias;
    private String dataStream;

    private static final String TEMPLATE = """
        {
            "index_patterns": ["%s*"],
            "template": {
                "settings":{
                    "index": {
                        "number_of_replicas": 0,
                        "number_of_shards": 1,
                        "mode": "time_series"
                    },
                    "index.lifecycle.name": "%s"
                },
                "mappings":{
                    "properties": {
                        "@timestamp" : {
                            "type": "date"
                        },
                        "metricset": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "volume": {
                            "type": "double",
                            "time_series_metric": "gauge"
                        }
                    }
                }
            },
            "data_stream": { }
        }""";

    @Before
    public void refreshAbstractions() {
        index = "index-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        alias = "alias-" + randomAlphaOfLength(5);
        dataStream = "ds-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        logger.info(
            "--> running [{}] with index [{}], data stream [{}], alias [{}] and policy [{}]",
            getTestName(),
            index,
            dataStream,
            alias,
            policy
        );
    }

    @Before
    public void updatePollInterval() throws IOException {
        updateClusterSettings(client(), Settings.builder().put("indices.lifecycle.poll_interval", "5s").build());
    }

    private void createIndex(String index, String alias, boolean isTimeSeries) throws IOException {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy);

        if (isTimeSeries) {
            settings.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("metricset"))
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2006-01-08T23:40:53.384Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z");
        }

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("metricset")
            .field("type", "keyword")
            .field("time_series_dimension", true)
            .endObject()
            .startObject("volume")
            .field("type", "double")
            .field("time_series_metric", "gauge")
            .endObject()
            .endObject()
            .endObject();
        String mapping = Strings.toString(builder);
        createIndexWithSettings(client(), index, alias, settings, mapping);
    }

    public void testRollupIndex() throws Exception {
        createIndex(index, alias, true);
        index(client(), index, true, null, "@timestamp", "2020-01-01T05:10:00Z", "volume", 11.0, "metricset", randomAlphaOfLength(5));

        String phaseName = randomFrom("warm", "cold");
        DateHistogramInterval fixedInterval = ConfigTestHelpers.randomInterval();
        createNewSingletonPolicy(client(), policy, phaseName, new DownsampleAction(fixedInterval, DEFAULT_TIMEOUT));
        updatePolicy(client(), index, policy);

        String rollupIndex = waitAndGetRollupIndexName(client(), index, fixedInterval);
        assertNotNull("Cannot retrieve rollup index name", rollupIndex);
        assertBusy(() -> assertTrue("Rollup index does not exist", indexExists(rollupIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertFalse("Source index should have been deleted", indexExists(index)), 30, TimeUnit.SECONDS);
        assertBusy(
            () -> assertThat(getStepKeyForIndex(client(), rollupIndex), equalTo(PhaseCompleteStep.finalStep(phaseName).getKey())),
            30,
            TimeUnit.SECONDS
        );
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), rollupIndex);
            assertEquals(index, settings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.getKey()));
            assertEquals(DownsampleTaskStatus.SUCCESS.toString(), settings.get(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey()));
        });
        assertBusy(
            () -> assertTrue("Alias [" + alias + "] does not point to index [" + rollupIndex + "]", aliasExists(rollupIndex, alias))
        );
    }

    public void testRollupIndexInTheHotPhase() throws Exception {
        createIndex(index, alias, true);
        index(client(), index, true, null, "@timestamp", "2020-01-01T05:10:00Z", "volume", 11.0, "metricset", randomAlphaOfLength(5));

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> createNewSingletonPolicy(
                client(),
                policy,
                "hot",
                new DownsampleAction(ConfigTestHelpers.randomInterval(), DEFAULT_TIMEOUT)
            )
        );
        assertTrue(
            e.getMessage()
                .contains("the [downsample] action(s) may not be used in the [hot] phase without an accompanying [rollover] action")
        );
    }

    public void testRollupIndexInTheHotPhaseAfterRollover() throws Exception {
        String originalIndex = index + "-000001";

        // add a policy
        DateHistogramInterval fixedInterval = ConfigTestHelpers.randomInterval();
        Map<String, LifecycleAction> hotActions = Map.of(
            RolloverAction.NAME,
            new RolloverAction(null, null, null, 1L, null, null, null, null, null, null),
            DownsampleAction.NAME,
            new DownsampleAction(fixedInterval, DEFAULT_TIMEOUT)
        );
        Map<String, Phase> phases = Map.of("hot", new Phase("hot", TimeValue.ZERO, hotActions));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, phases);
        Request createPolicyRequest = new Request("PUT", "_ilm/policy/" + policy);
        createPolicyRequest.setJsonEntity("{ \"policy\":" + Strings.toString(lifecyclePolicy) + "}");
        client().performRequest(createPolicyRequest);

        // and a template
        Request createTemplateRequest = new Request("PUT", "_template/" + index);
        createTemplateRequest.setJsonEntity(Strings.format("""
            {
              "index_patterns": ["%s-*"],
              "settings": {
                "number_of_shards": %s,
                "number_of_replicas": 0,
                "index.lifecycle.name": "%s",
                "index.lifecycle.rollover_alias": "%s"
              }
            }""", index, 1, policy, alias));
        createTemplateRequest.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        client().performRequest(createTemplateRequest);

        // then create the index and index a document to trigger rollover
        createIndex(originalIndex, alias, true);
        index(
            client(),
            originalIndex,
            true,
            null,
            "@timestamp",
            "2020-01-01T05:10:00Z",
            "volume",
            11.0,
            "metricset",
            randomAlphaOfLength(5)
        );

        String rollupIndex = waitAndGetRollupIndexName(client(), originalIndex, fixedInterval);
        assertNotNull("Cannot retrieve rollup index name", rollupIndex);
        assertBusy(() -> assertTrue("Rollup index does not exist", indexExists(rollupIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertFalse("Source index should have been deleted", indexExists(originalIndex)), 30, TimeUnit.SECONDS);
        assertBusy(
            () -> assertThat(getStepKeyForIndex(client(), rollupIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())),
            30,
            TimeUnit.SECONDS
        );
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), rollupIndex);
            assertEquals(originalIndex, settings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.getKey()));
            assertEquals(DownsampleTaskStatus.SUCCESS.toString(), settings.get(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey()));
        });
    }

    public void testTsdbDataStreams() throws Exception {
        // Create the ILM policy
        DateHistogramInterval fixedInterval = ConfigTestHelpers.randomInterval();
        createNewSingletonPolicy(client(), policy, "warm", new DownsampleAction(fixedInterval, DEFAULT_TIMEOUT));

        // Create a template
        Request createIndexTemplateRequest = new Request("POST", "/_index_template/" + dataStream);
        createIndexTemplateRequest.setJsonEntity(Strings.format(TEMPLATE, dataStream, policy));
        assertOK(client().performRequest(createIndexTemplateRequest));

        String now = DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(Instant.now());
        index(client(), dataStream, true, null, "@timestamp", now, "volume", 11.0, "metricset", randomAlphaOfLength(5));

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1);
        assertBusy(
            () -> assertThat(
                "index must wait in the " + CheckNotDataStreamWriteIndexStep.NAME + " until it is not the write index anymore",
                explainIndex(client(), backingIndexName).get("step"),
                is(CheckNotDataStreamWriteIndexStep.NAME)
            ),
            30,
            TimeUnit.SECONDS
        );

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        String rollupIndex = waitAndGetRollupIndexName(client(), backingIndexName, fixedInterval);
        assertNotNull("Cannot retrieve rollup index name", rollupIndex);
        assertBusy(() -> assertTrue("Rollup index does not exist", indexExists(rollupIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertFalse("Source index should have been deleted", indexExists(backingIndexName)), 30, TimeUnit.SECONDS);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), rollupIndex);
            assertEquals(backingIndexName, settings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.getKey()));
            assertEquals(DownsampleTaskStatus.SUCCESS.toString(), settings.get(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey()));
        });
    }

    public void testRollupNonTSIndex() throws Exception {
        createIndex(index, alias, false);
        index(client(), index, true, null, "@timestamp", "2020-01-01T05:10:00Z", "volume", 11.0, "metricset", randomAlphaOfLength(5));

        String phaseName = randomFrom("warm", "cold");
        DateHistogramInterval fixedInterval = ConfigTestHelpers.randomInterval();
        createNewSingletonPolicy(client(), policy, phaseName, new DownsampleAction(fixedInterval, DEFAULT_TIMEOUT));
        updatePolicy(client(), index, policy);

        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep(phaseName).getKey())));
        String rollupIndex = getRollupIndexName(client(), index, fixedInterval);
        assertNull("Rollup index should not have been created", rollupIndex);
        assertTrue("Source index should not have been deleted", indexExists(index));
    }

    /**
     * Gets the generated rollup index name for a given index by looking at newly created indices that match the rollup index name pattern
     *
     * @param originalIndexName the name of the source index used to generate the rollup index name
     * @return the name of the rollup index for a given index, null if none exist
     */
    public String waitAndGetRollupIndexName(RestClient client, String originalIndexName, DateHistogramInterval fixedInterval)
        throws InterruptedException {
        final String[] rollupIndexName = new String[1];
        waitUntil(() -> {
            try {
                rollupIndexName[0] = getRollupIndexName(client, originalIndexName, fixedInterval);
                return rollupIndexName[0] != null;
            } catch (IOException e) {
                return false;
            }
        }, 60, TimeUnit.SECONDS);
        logger.info("--> original index name is [{}], rollup index name is [{}]", originalIndexName, rollupIndexName[0]);
        return rollupIndexName[0];
    }

    public static String getRollupIndexName(RestClient client, String originalIndexName, DateHistogramInterval fixedInterval)
        throws IOException {
        String endpoint = "/"
            + DownsampleAction.DOWNSAMPLED_INDEX_PREFIX
            + "*-"
            + originalIndexName
            + "-"
            + fixedInterval
            + "/?expand_wildcards=all";
        Response response = client.performRequest(new Request("GET", endpoint));
        Map<String, Object> asMap = responseAsMap(response);
        if (asMap.size() == 1) {
            return (String) asMap.keySet().toArray()[0];
        }
        return null;
    }
}
