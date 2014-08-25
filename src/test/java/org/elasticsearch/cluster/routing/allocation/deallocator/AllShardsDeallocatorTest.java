/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.elasticsearch.cluster.routing.allocation.deallocator;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 3)
public class AllShardsDeallocatorTest extends DeallocatorTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
        Loggers.getLogger(AllShardsDeallocator.class).setLevel("TRACE");
        System.setProperty(TESTS_CLUSTER, ""); // ensure InternalTestCluster
    }

    @Test
    public void testDeallocate() throws Exception {
        createIndices();

        AllShardsDeallocator allShardsDeallocator = ((InternalTestCluster)cluster()).getInstance(AllShardsDeallocator.class, takeDownNode.name());
        ListenableFuture<Deallocator.DeallocationResult> future = allShardsDeallocator.deallocate();
        Deallocator.DeallocationResult result = future.get(1, TimeUnit.MINUTES);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(true));


        assertThat(
                ((InternalTestCluster)cluster()).getInstance(ClusterService.class, takeDownNode.name()).state().routingNodes().node(takeDownNode.id()).size(),
                is(0));
        ClusterHealthStatus status = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("2s").execute().actionGet().getStatus();
        assertThat(status, is(ClusterHealthStatus.GREEN));
    }

    @Test
    public void testDeallocateAllocationEnableSetting() throws Exception {
        createIndices();

        cluster().client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                ImmutableSettings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, EnableAllocationDecider.Allocation.NONE.name())
        ).execute().actionGet();

        AllShardsDeallocator allShardsDeallocator = ((InternalTestCluster)cluster()).getInstance(AllShardsDeallocator.class, takeDownNode.name());
        ListenableFuture<Deallocator.DeallocationResult> future = allShardsDeallocator.deallocate();
        Deallocator.DeallocationResult result = future.get(1, TimeUnit.MINUTES);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(true));

        ClusterHealthStatus status = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("2s").execute().actionGet().getStatus();
        assertThat(status, is(ClusterHealthStatus.GREEN));

        waitFor(new Predicate<Void>() {
            @Override
            public boolean apply(Void aVoid) {
                ClusterState newState = ((InternalTestCluster)cluster()).getInstance(ClusterService.class, takeDownNode.name()).state();

                return newState.metaData().settings().get(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "").equals(EnableAllocationDecider.Allocation.NONE.name()) &&
                    newState.routingNodes().node(takeDownNode.id()).size() == 0;
            }
        }, 100);
    }

    @Test
    public void testDeallocateFailCannotMoveShards() throws Exception {
        client().admin().indices()
                .prepareCreate("t2")
                .addMapping("default", mappingSource)
                .setSettings(ImmutableSettings.builder().put("number_of_shards", 2).put("number_of_replicas", 2))
                .execute().actionGet();
        ensureGreen();

        for (int i = 0; i < 4; i++) {
            client().prepareIndex("t2", "default")
                    .setId(String.valueOf(randomInt()))
                    .setSource(ImmutableMap.<String, Object>of("name", randomAsciiOfLength(10))).execute().actionGet();

        }
        refresh();

        AllShardsDeallocator allShardsDeallocator = ((InternalTestCluster)cluster()).getInstance(AllShardsDeallocator.class, takeDownNode.name());
        ListenableFuture<Deallocator.DeallocationResult> future = allShardsDeallocator.deallocate();
        try {
            future.get(2, TimeUnit.SECONDS);
            fail("no TimeoutException occurred");
        } catch (TimeoutException e) {
            assertThat(clusterService().state().routingNodes().node(takeDownNode.id()).size(), is(2));
        }
    }

    @Test
    public void testCancelAllocationEnableSetting() throws Exception {
        createIndices();

        cluster().client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                ImmutableSettings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, EnableAllocationDecider.Allocation.NONE.name())
        ).execute().actionGet();

        AllShardsDeallocator allShardsDeallocator = ((InternalTestCluster)cluster()).getInstance(AllShardsDeallocator.class, takeDownNode.name());
        assertThat(allShardsDeallocator.cancel(), is(false));
        ListenableFuture<Deallocator.DeallocationResult> future = allShardsDeallocator.deallocate();
        assertThat(allShardsDeallocator.isDeallocating(), is(true));
        assertThat(allShardsDeallocator.cancel(), is(true));
        assertThat(allShardsDeallocator.isDeallocating(), is(false));

        try {
            future.get(1, TimeUnit.SECONDS);
            fail("no CancellationException thrown");
        } catch (CancellationException e) {
            // glad we reached this point
        }

        waitFor(new Predicate<Void>() {
            @Override
            public boolean apply(Void aVoid) {
                ClusterState newState = ((InternalTestCluster)cluster()).getInstance(ClusterService.class, takeDownNode.name()).state();
                return newState.metaData().settings().get(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "").equals(EnableAllocationDecider.Allocation.NONE.name());
            }
        }, 100);
    }

    @Test
    public void testCancel() throws Exception {
        createIndices();

        AllShardsDeallocator allShardsDeallocator = ((InternalTestCluster)cluster()).getInstance(AllShardsDeallocator.class, takeDownNode.name());
        assertThat(allShardsDeallocator.cancel(), is(false));
        ListenableFuture<Deallocator.DeallocationResult> future = allShardsDeallocator.deallocate();
        assertThat(allShardsDeallocator.isDeallocating(), is(true));
        assertThat(allShardsDeallocator.cancel(), is(true));
        assertThat(allShardsDeallocator.isDeallocating(), is(false));

        assertThat(future.isDone(), is(true));
        assertThat(future.isCancelled(), is(true));

        expectedException.expect(CancellationException.class);

        future.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testDeallocationNoOps() throws Exception {
        AllShardsDeallocator allShardsDeallocator = ((InternalTestCluster)cluster()).getInstance(AllShardsDeallocator.class, takeDownNode.name());

        // NOOP
        Deallocator.DeallocationResult result = allShardsDeallocator.deallocate().get(1, TimeUnit.SECONDS);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(false));

        createIndices();

        // DOUBLE deallocation
        allShardsDeallocator.deallocate();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("node already waiting for complete deallocation");
        allShardsDeallocator.deallocate();
    }
}
