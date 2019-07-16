/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.condition.AlwaysCondition;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * This rolling upgrade node tests whether watcher is able to update the watch status after execution in a mixed cluster.
 *
 * Versions before 6.7.0 the watch status was using the version to do optimistic locking, after 6.7.0 sequence number and
 * primary term are used. The problem was that bwc logic was forgotten to be added, so in a mixed versions cluster, when
 * a watch is executed and its watch status is updated then an update request using sequence number / primary term as
 * way to do optimistic locking can be sent to nodes that don't support this.
 *
 * This test tries to simulate a situation where the bug manifests. This requires watches to be run by multiple nodes
 * holding a .watches index shard.
 */
public class WatcherUpgradeIT extends AbstractUpgradeTestCase {

    public void testWatchesKeepRunning() throws Exception {
        if (UPGRADED_FROM_VERSION.before(Version.V_6_0_0)) {
            logger.info("Skipping test. Upgrading from before 6.0 makes this test too complicated.");
            return;
        }

        final int numWatches = 16;

        if (CLUSTER_TYPE.equals(ClusterType.OLD)) {
            final String watch = watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(simpleInput())
                .condition(AlwaysCondition.INSTANCE)
                .addAction("_action1", loggingAction("{{ctx.watch_id}}"))
                .buildAsBytes(XContentType.JSON)
                .utf8ToString();

            for (int i = 0; i < numWatches; i++) {
                // Using a random id helps to distribute the watches between watcher services on the different nodes with
                // a .watches index shard:
                String id = UUIDs.randomBase64UUID();
                logger.info("Adding watch [{}/{}]", id, Math.floorMod(Murmur3HashFunction.hash(id), 3));
                Request putWatchRequest = new Request("PUT", "/_xpack/watcher/watch/" + id);
                putWatchRequest.setJsonEntity(watch);
                assertOK(client().performRequest(putWatchRequest));

                if (i == 0) {
                    // Increasing the number of replicas to makes it more likely that an upgraded node sends an
                    // update request (in order to update watch status) to a non upgraded node.
                    Request updateSettingsRequest = new Request("PUT", "/.watches/_settings");
                    updateSettingsRequest.setJsonEntity(Strings.toString(Settings.builder()
                        .put("index.number_of_replicas", 2)
                        .put("index.auto_expand_replicas", (String) null)
                        .build()));
                    assertOK(client().performRequest(updateSettingsRequest));
                    ensureAllWatchesIndexShardsStarted();
                }
            }
        } else {
            ensureAllWatchesIndexShardsStarted();
            // Restarting watcher helps to ensure that after a node upgrade each node will be executing watches:
            // (and not that a non upgraded node is in charge of watches that an upgraded node should run)
            assertOK(client().performRequest(new Request("POST", "/_xpack/watcher/_stop")));
            assertOK(client().performRequest(new Request("POST", "/_xpack/watcher/_start")));

            // Casually checking whether watches are executing:
            for (int i = 0; i < 10; i++) {
                int previous = getWatchHistoryEntriesCount();
                assertBusy(() -> {
                    Integer totalHits = getWatchHistoryEntriesCount();
                    assertThat(totalHits, greaterThan(previous));
                }, 30, TimeUnit.SECONDS);
            }
        }
    }

    private int getWatchHistoryEntriesCount() throws IOException {
        Request refreshRequest = new Request("POST", "/.watcher-history-*/_refresh");
        assertOK(client().performRequest(refreshRequest));

        Request searchRequest = new Request("GET", "/.watcher-history-*/_search");
        searchRequest.setJsonEntity("{\"query\": {\"match\": {\"state\": {\"query\": \"executed\"}}}}");

        Response response = client().performRequest(searchRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> responseBody = entityAsMap(response);
        return (Integer) ((Map<?, ?>) responseBody.get("hits")).get("total");
    }

    private void ensureAllWatchesIndexShardsStarted() throws Exception {
        assertBusy(() -> {
            Request request = new Request("GET", "/_cluster/health/.watches");
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
            Map<String, Object> responseBody = entityAsMap(response);
            int activeShards = (int) responseBody.get("active_shards");
            assertThat(activeShards, equalTo(3));
        }, 30, TimeUnit.SECONDS);
    }

}
