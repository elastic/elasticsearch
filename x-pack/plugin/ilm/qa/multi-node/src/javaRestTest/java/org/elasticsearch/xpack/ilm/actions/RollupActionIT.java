/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.actions;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.RollupILMAction;
import org.elasticsearch.xpack.core.ilm.RollupStep;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.junit.Before;

import java.util.Collections;
import java.util.Locale;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createIndexWithSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getOnlyIndexSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.index;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.updatePolicy;
import static org.hamcrest.Matchers.equalTo;

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
        String rollupIndex = RollupStep.getRollupIndexName(index);
        index(client(), index, "_id", "timestamp", "2020-01-01T05:10:00Z", "volume", 11.0);
        RollupActionConfig rollupConfig = new RollupActionConfig(
            new RollupActionGroupConfig(new RollupActionDateHistogramGroupConfig.FixedInterval("timestamp", DateHistogramInterval.DAY)),
            Collections.singletonList(new MetricConfig("volume", Collections.singletonList("max"))));

        createNewSingletonPolicy(client(), policy, "cold", new RollupILMAction(rollupConfig, null));
        updatePolicy(client(), index, policy);

        assertBusy(() -> assertTrue(indexExists(rollupIndex)));
        assertBusy(() -> assertFalse(getOnlyIndexSettings(client(), rollupIndex).containsKey(LifecycleSettings.LIFECYCLE_NAME)));
        assertBusy(() -> assertTrue(indexExists(index)));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/68609")
    public void testRollupIndexAndSetNewRollupPolicy() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        String rollupIndex = RollupStep.ROLLUP_INDEX_NAME_PREFIX + index;
        index(client(), index, "_id", "timestamp", "2020-01-01T05:10:00Z", "volume", 11.0);
        RollupActionConfig rollupConfig = new RollupActionConfig(
            new RollupActionGroupConfig(new RollupActionDateHistogramGroupConfig.FixedInterval("timestamp", DateHistogramInterval.DAY)),
            Collections.singletonList(new MetricConfig("volume", Collections.singletonList("max"))));

        createNewSingletonPolicy(client(), policy, "cold", new RollupILMAction(rollupConfig, policy));
        updatePolicy(client(), index, policy);

        assertBusy(() -> assertTrue(indexExists(rollupIndex)));
        assertBusy(() -> assertThat(getOnlyIndexSettings(client(), rollupIndex).get(LifecycleSettings.LIFECYCLE_NAME), equalTo(policy)));
        assertBusy(() -> assertTrue(indexExists(index)));
    }

}
