/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class SimpleAllocationIT extends ESIntegTestCase {

    @Override
    protected int numberOfShards() {
        return 3;
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    public void testSaneAllocation() {
        assertAcked(prepareCreate("test", 3));
        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test"));
        }
        ensureGreen("test");

        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(0));
        for (RoutingNode node : state.getRoutingNodes()) {
            if (node.isEmpty() == false) {
                assertThat(node.size(), equalTo(2));
            }
        }
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 0))
            .execute()
            .actionGet();
        ensureGreen("test");
        state = client().admin().cluster().prepareState().execute().actionGet().getState();

        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(0));
        for (RoutingNode node : state.getRoutingNodes()) {
            if (node.isEmpty() == false) {
                assertThat(node.size(), equalTo(1));
            }
        }

        // create another index
        assertAcked(prepareCreate("test2", 3));
        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test2"));
        }
        ensureGreen("test2");

        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 1))
            .execute()
            .actionGet();
        ensureGreen("test");
        state = client().admin().cluster().prepareState().execute().actionGet().getState();

        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(0));
        for (RoutingNode node : state.getRoutingNodes()) {
            if (node.isEmpty() == false) {
                assertThat(node.size(), equalTo(4));
            }
        }
    }
}
