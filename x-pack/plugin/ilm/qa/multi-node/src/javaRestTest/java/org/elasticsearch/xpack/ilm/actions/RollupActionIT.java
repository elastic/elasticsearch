/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.actions;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.RollupILMAction;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createIndexWithSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getOnlyIndexSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.index;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.updatePolicy;
import static org.hamcrest.Matchers.equalTo;

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/68609")
public class RollupActionIT extends ESRestTestCase {

    private String index;
    private String policy;
    private String alias;

    @Before
    public void refreshIndex() {
        index = "index-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        alias = "alias-" + randomAlphaOfLength(5);
        logger.info("--> running [{}] with index [{}], alias [{}] and policy [{}]", getTestName(), index, alias, policy);
    }

    public void testRollupIndex() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        index(client(), index, "_id", "timestamp", "2020-01-01T05:10:00Z", "volume", 11.0);
        RollupActionConfig rollupConfig = new RollupActionConfig(
            new RollupActionGroupConfig(new RollupActionDateHistogramGroupConfig.FixedInterval("timestamp", DateHistogramInterval.DAY)),
            Collections.singletonList(new MetricConfig("volume", Collections.singletonList("max"))));

        createNewSingletonPolicy(client(), policy, "cold", new RollupILMAction(rollupConfig, null));
        updatePolicy(client(), index, policy);

        assertBusy(() -> assertNotNull(getRollupIndexName(index)));
        String rollupIndex = getRollupIndexName(index);
        assertBusy(() -> assertTrue(indexExists(rollupIndex)));
        assertBusy(() -> assertFalse(getOnlyIndexSettings(client(), rollupIndex).containsKey(LifecycleSettings.LIFECYCLE_NAME)));
        assertBusy(() -> assertTrue(indexExists(index)));
    }

    public void testRollupIndexAndSetNewRollupPolicy() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        index(client(), index, "_id", "timestamp", "2020-01-01T05:10:00Z", "volume", 11.0);
        RollupActionConfig rollupConfig = new RollupActionConfig(
            new RollupActionGroupConfig(new RollupActionDateHistogramGroupConfig.FixedInterval("timestamp", DateHistogramInterval.DAY)),
            Collections.singletonList(new MetricConfig("volume", Collections.singletonList("max"))));

        createNewSingletonPolicy(client(), policy, "cold", new RollupILMAction(rollupConfig, policy));
        updatePolicy(client(), index, policy);

        assertBusy(() -> assertNotNull(getRollupIndexName(index)));
        String rollupIndex = getRollupIndexName(index);
        assertBusy(() -> assertTrue(indexExists(rollupIndex)));
        assertBusy(() -> assertThat(getOnlyIndexSettings(client(), rollupIndex).get(LifecycleSettings.LIFECYCLE_NAME), equalTo(policy)));
        assertBusy(() -> assertTrue(indexExists(index)));
    }

    /**
     * gets the generated rollup index name for a given index by looking at newly created indices that match the rollup index name pattern
     *
     * @param index the name of the source index used to generate the rollup index name
     * @return the name of the rollup index for a given index, null if none exist
     * @throws IOException if request fails
     */
    private String getRollupIndexName(String index) throws IOException {
        Response response = client().performRequest(new Request("GET", "/rollup-*-" + index));
        Map<String, Object> asMap = responseAsMap(response);
        if (asMap.size() == 1) {
            return (String) asMap.keySet().toArray()[0];
        }
        return null;
    }
}
