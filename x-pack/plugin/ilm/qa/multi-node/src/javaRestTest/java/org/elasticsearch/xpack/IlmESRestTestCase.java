/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

public abstract class IlmESRestTestCase extends ESRestTestCase {
    static final String USER = "user";
    static final String PASSWORD = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-ilm")
        .module("searchable-snapshots")
        .module("data-streams")
        .nodes(4)
        .setting("path.repo", () -> createTempDir("repo").toAbsolutePath().toString())
        .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
        .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("indices.lifecycle.poll_interval", "1000ms")
        .setting("logger.org.elasticsearch.xpack.core.ilm", "TRACE")
        .setting("logger.org.elasticsearch.xpack.ilm", "TRACE")
        // The TRACE logs of the history store are too verbose and not useful, so we set it to INFO
        .setting("logger.org.elasticsearch.xpack.ilm.history.ILMHistoryStore", "INFO")
        /*
         * In TimeSeriesLifecycleActionsIT.testWaitForSnapshotSlmExecutedBefore() we create a snapshot, then associate an ILM policy with
         * an index, and then that policy checks if a snapshot has been started at the same millisecond or later than the policy's action's
         * date. Since both the snapshot start time and policy are using ThreadPool.absoluteTimeInMillis(), it is possible that they get
         * the same cached result back (it is kept for about 200 ms). The following config changes ThreadPool.absoluteTimeInMillis() to
         * always use System.currentTimeMillis() rather than a cached time. So the policy's action date is always after the snapshot's
         * start.
         */
        .setting("thread_pool.estimated_time_interval", "0")
        .setting("time_series.poll_interval", "10m")
        // Disable shard balancing to avoid force merges failing due to relocating shards.
        .setting("cluster.routing.rebalance.enable", "none")
        .user(USER, PASSWORD)
        .build();

    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
