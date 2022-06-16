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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ilm.CheckNotDataStreamWriteIndexStep;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.RollupILMAction;
import org.elasticsearch.xpack.core.rollup.RollupActionConfigTests;
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
import static org.elasticsearch.xpack.TimeSeriesRestDriver.index;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.rolloverMaxOneDocCondition;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.updatePolicy;
import static org.hamcrest.Matchers.is;

public class RollupActionIT extends ESRestTestCase {

    private String index;
    private String policy;
    private String alias;

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
        logger.info("--> running [{}] with index [{}], alias [{}] and policy [{}]", getTestName(), index, alias, policy);
    }

    private void createIndex(String index, String alias) throws IOException {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("metricset"))
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2006-01-08T23:40:53.384Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z")
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy);

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
        createIndex(index, alias);
        index(client(), index, "", "@timestamp", "2020-01-01T05:10:00Z", "volume", 11.0, "metricset", randomAlphaOfLength(5));

        String phaseName = randomFrom("warm", "cold");
        createNewSingletonPolicy(client(), policy, phaseName, new RollupILMAction(RollupActionConfigTests.randomConfig()));
        updatePolicy(client(), index, policy);

        assertBusy(() -> assertNotNull("Cannot retrieve rollup index name", getRollupIndexName(index)));
        String rollupIndex = getRollupIndexName(index);
        assertBusy(() -> assertTrue("Rollup index does not exist", indexExists(rollupIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertFalse("Source index should have been deleted", indexExists(index)), 30, TimeUnit.SECONDS);
    }

    public void testRollupIndexInTheHotPhase() throws Exception {
        createIndex(index, alias);
        index(client(), index, "", "@timestamp", "2020-01-01T05:10:00Z", "volume", 11.0, "metricset", randomAlphaOfLength(5));

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> createNewSingletonPolicy(client(), policy, "hot", new RollupILMAction(RollupActionConfigTests.randomConfig()))
        );
        assertTrue(
            e.getMessage().contains("the [rollup] action(s) may not be used in the [hot] phase without an accompanying [rollover] action")
        );
    }

    public void testRollupIndexInTheHotPhaseAfterRollover() throws Exception {
        String originalIndex = index + "-000001";

        // add a policy
        Map<String, LifecycleAction> hotActions = Map.of(
            RolloverAction.NAME,
            new RolloverAction(null, null, null, 1L, null),
            RollupILMAction.NAME,
            new RollupILMAction(RollupActionConfigTests.randomConfig())
        );
        Map<String, Phase> phases = Map.of("hot", new Phase("hot", TimeValue.ZERO, hotActions));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, phases);
        Request createPolicyRequest = new Request("PUT", "_ilm/policy/" + policy);
        createPolicyRequest.setJsonEntity("{ \"policy\":" + Strings.toString(lifecyclePolicy) + "}");
        client().performRequest(createPolicyRequest);

        // and a template
        Request createTemplateRequest = new Request("PUT", "_template/" + index);
        createTemplateRequest.setJsonEntity("""
            {
              "index_patterns": ["%s-*"],
              "settings": {
                "number_of_shards": %s,
                "number_of_replicas": 0,
                "index.lifecycle.name": "%s",
                "index.lifecycle.rollover_alias": "%s"
              }
            }""".formatted(index, 1, policy, alias));
        createTemplateRequest.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        client().performRequest(createTemplateRequest);

        // then create the index and index a document to trigger rollover
        createIndex(originalIndex, alias);
        index(client(), originalIndex, "", "@timestamp", "2020-01-01T05:10:00Z", "volume", 11.0, "metricset", randomAlphaOfLength(5));

        assertBusy(() -> assertNotNull("Cannot retrieve rollup index name", getRollupIndexName(originalIndex)), 30, TimeUnit.SECONDS);
        String rollupIndex = getRollupIndexName(originalIndex);

        assertBusy(() -> assertTrue("Rollup index does not exist", indexExists(rollupIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertFalse("Source index should have been deleted", indexExists(originalIndex)), 30, TimeUnit.SECONDS);
        // assertBusy(() -> assertThat(getStepKeyForIndex(client(), rollupIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

    public void testTsdbDataStreams() throws Exception {
        final String dataStream = "k8s-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        // Create the ILM policy
        createNewSingletonPolicy(client(), policy, "warm", new RollupILMAction(RollupActionConfigTests.randomConfig()));

        // Create a template
        Request createIndexTemplateRequest = new Request("POST", "/_index_template/" + dataStream);
        createIndexTemplateRequest.setJsonEntity(TEMPLATE.formatted(dataStream, policy));
        assertOK(client().performRequest(createIndexTemplateRequest));

        String now = DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(Instant.now());
        index(client(), dataStream, "", "@timestamp", now, "volume", 11.0, "metricset", randomAlphaOfLength(5));

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
        // assertBusy(() -> assertNotNull("Cannot retrieve rollup index name", getRollupIndexName(backingIndexName)), 30, TimeUnit.SECONDS);
        waitUntil(() -> {
            try {
                String rollupIndex = getRollupIndexName(backingIndexName);
                return rollupIndex != null;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS);
        String rollupIndex = getRollupIndexName(backingIndexName);

        assertBusy(() -> assertTrue("Rollup index does not exist", indexExists(rollupIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertFalse("Source index should have been deleted", indexExists(backingIndexName)), 30, TimeUnit.SECONDS);
    }

    /**
     * gets the generated rollup index name for a given index by looking at newly created indices that match the rollup index name pattern
     *
     * @param index the name of the source index used to generate the rollup index name
     * @return the name of the rollup index for a given index, null if none exist
     * @throws IOException if request fails
     */
    private String getRollupIndexName(String index) throws IOException {
        Response response = client().performRequest(
            new Request("GET", "/" + RollupILMAction.ROLLUP_INDEX_PREFIX + "*-" + index + "/?expand_wildcards=all")
        );
        Map<String, Object> asMap = responseAsMap(response);
        if (asMap.size() == 1) {
            return (String) asMap.keySet().toArray()[0];
        }
        return null;
    }
}
