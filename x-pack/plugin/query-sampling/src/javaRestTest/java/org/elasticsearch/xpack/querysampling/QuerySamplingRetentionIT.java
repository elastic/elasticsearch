/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.nullValue;

/**
 * Checks that sampled queries do not stay in the index forever. The retention of the cluster is short, which is why
 * this has a cluster of its own: other tests could not rely on what they sampled to still be there.
 */
public class QuerySamplingRetentionIT extends QuerySamplingRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-query-sampling")
        .module("reindex")
        .feature(FeatureFlag.QUERY_SAMPLING)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.query_sampling.retention", "5s")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testSampledQueriesAreDeletedOnceTheyAreOlderThanTheRetention() throws Exception {
        setUpIndexAndSampling();
        float x = sampleOneQuery();

        // the retention is checked every second and a bit, and counted from when the query was written
        assertBusy(() -> assertThat(storedValue(x, "weighted_multiplicity"), nullValue()), 30, TimeUnit.SECONDS);
    }
}
