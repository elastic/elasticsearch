/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.actions;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ilm.RollupILMAction;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionConfigTests;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.index;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.updatePolicy;

public class RollupActionIT extends ESRestTestCase {

    private String index;
    private String policy;
    private String alias;

    @Before
    public void refreshIndex() throws IOException {
        index = "index-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        alias = "alias-" + randomAlphaOfLength(5);
        logger.info("--> running [{}] with index [{}], alias [{}] and policy [{}]", getTestName(), index, alias, policy);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("dim_field1"))
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2006-01-08T23:40:53.384Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z")
            .build();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("dim_field1")
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
        ESRestTestCase.createIndex(client(), index, settings, mapping, null);
        index(client(), index, "", "@timestamp", "2020-01-01T05:10:00Z", "volume", 11.0, "dim_field1", randomAlphaOfLength(5));
    }

    public void testRollupIndex() throws Exception {
        RollupActionConfig rollupConfig = RollupActionConfigTests.randomConfig();
        String phaseName = randomFrom("warm", "cold");
        createNewSingletonPolicy(client(), policy, phaseName, new RollupILMAction(rollupConfig));
        updatePolicy(client(), index, policy);

        assertBusy(() -> assertNotNull("Cannot retrieve rollup index name", getRollupIndexName(index)));
        String rollupIndex = getRollupIndexName(index);
        assertBusy(() -> assertTrue("Rollup index does not exist", indexExists(rollupIndex)));
        assertBusy(() -> assertFalse("Source index should have been deleted", indexExists(index)));
    }

    // public void testRollupIndexInTheHotPhase() throws Exception {
    // RollupActionConfig rollupConfig = RollupActionConfigTests.randomConfig();
    // createNewSingletonPolicy(client(), policy, "hot", new RollupILMAction(rollupConfig));
    // updatePolicy(client(), index, policy);
    //
    // assertBusy(() -> assertNotNull(getRollupIndexName(index)));
    // String rollupIndex = getRollupIndexName(index);
    // assertTrue(indexExists(rollupIndex));
    // assertFalse(indexExists(index));
    // }

    /**
     * gets the generated rollup index name for a given index by looking at newly created indices that match the rollup index name pattern
     *
     * @param index the name of the source index used to generate the rollup index name
     * @return the name of the rollup index for a given index, null if none exist
     * @throws IOException if request fails
     */
    private String getRollupIndexName(String index) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + RollupILMAction.ROLLUP_INDEX_PREFIX + "*-" + index));
        Map<String, Object> asMap = responseAsMap(response);
        if (asMap.size() == 1) {
            return (String) asMap.keySet().toArray()[0];
        }
        return null;
    }
}
