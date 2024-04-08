/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests setting the active shard count for replication operations (e.g. index) operates correctly.
 */
public class WaitActiveShardCountIT extends ESIntegTestCase {
    public void testReplicationWaitsForActiveShardCount() throws Exception {
        CreateIndexResponse createIndexResponse = prepareCreate("test", 1, indexSettings(1, 2)).get();

        assertAcked(createIndexResponse);

        // indexing, by default, will work (waiting for one shard copy only)
        prepareIndex("test").setId("1").setSource(source("1", "test"), XContentType.JSON).get();
        try {
            prepareIndex("test").setId("1")
                .setSource(source("1", "test"), XContentType.JSON)
                .setWaitForActiveShards(2) // wait for 2 active shard copies
                .setTimeout(timeValueMillis(100))
                .get();
            fail("can't index, does not enough active shard copies");
        } catch (UnavailableShardsException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertThat(
                e.getMessage(),
                startsWith("[test][0] Not enough active copies to meet shard count of [2] (have 1, needed 2). Timeout: [100ms], request:")
            );
            // but really, all is well
        }

        allowNodes("test", 2);

        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForActiveShards(2)
            .setWaitForYellowStatus()
            .get();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        // this should work, since we now have two
        prepareIndex("test").setId("1")
            .setSource(source("1", "test"), XContentType.JSON)
            .setWaitForActiveShards(2)
            .setTimeout(timeValueSeconds(1))
            .get();

        try {
            prepareIndex("test").setId("1")
                .setSource(source("1", "test"), XContentType.JSON)
                .setWaitForActiveShards(ActiveShardCount.ALL)
                .setTimeout(timeValueMillis(100))
                .get();
            fail("can't index, not enough active shard copies");
        } catch (UnavailableShardsException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertThat(
                e.getMessage(),
                startsWith(
                    "[test][0] Not enough active copies to meet shard count of ["
                        + ActiveShardCount.ALL
                        + "] (have 2, needed 3). Timeout: [100ms], request:"
                )
            );
            // but really, all is well
        }

        allowNodes("test", 3);
        clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForActiveShards(3)
            .setWaitForGreenStatus()
            .get();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        // this should work, since we now have all shards started
        prepareIndex("test").setId("1")
            .setSource(source("1", "test"), XContentType.JSON)
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .setTimeout(timeValueSeconds(1))
            .get();
    }

    private String source(String id, String nameValue) {
        return Strings.format("""
            { "type1" : { "id" : "%s", "name" : "%s" } }
            """, id, nameValue);
    }
}
