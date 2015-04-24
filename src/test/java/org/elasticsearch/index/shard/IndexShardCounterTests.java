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


package org.elasticsearch.index.shard;

import com.google.common.base.Predicate;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;


public class IndexShardCounterTests extends ElasticsearchSingleNodeTest {

    @Test
    public void basicShardRefCounterTest() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        IndexShard indexShard = indexService.shard(0);
        assertThat(indexShard.getOperationCounter(), equalTo(1));
        indexShard.incrementOperationCounter();
        assertThat(indexShard.getOperationCounter(), equalTo(2));
        indexShard.decrementOperationCounter();
        assertThat(indexShard.getOperationCounter(), equalTo(1));
        indexShard.incrementOperationCounter();
        assertThat(indexShard.getOperationCounter(), equalTo(2));
        indexShard.decrementOperationCounter();
        assertThat(indexShard.getOperationCounter(), equalTo(1));
    }

    @Test
    public void testCounterDecrementedEvenIfWeGetExceptions() throws InterruptedException, ExecutionException, IOException {
        createIndex("test");
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        final IndexShard indexShard = indexService.shard(0);
        ExceptionThrowingIndexAction exceptionThrowingTransportIndexAction = getExceptionThrowingIndexAction();
        try {
            exceptionThrowingTransportIndexAction.execute(new IndexRequest("test", "type", "1").source("{\"foo\":\"bar\"}").timeout("100ms")).get();
            fail();
        } catch (Throwable t) {
            // expected
            logger.info("exception was", t);
        }
        assertThat(indexShard.getOperationCounter(), equalTo(1));
    }

    @Test
    public void testDeleteIndexDecreasesCounter() throws InterruptedException, ExecutionException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        IndexShard indexShard = indexService.shard(0);
        client().admin().indices().prepareDelete("test").get();
        assertThat(indexShard.getOperationCounter(), equalTo(0));
        try {
            indexShard.incrementOperationCounter();
            fail("we should not be able to increment anymore");
        } catch (IndexShardClosedException e) {
            // expected
        }
    }

    @Test
    public void testCounterStaysIncrementedWhileIndexingIsNotFinished() throws InterruptedException, ExecutionException, IOException {
        createIndex("test");
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        final IndexShard indexShard = indexService.shard(0);
        DelayedTransportIndexAction delayedTransportIndexAction = getDelayedTransportIndexAction();
        Future<IndexResponse> indexResponse = delayedTransportIndexAction.execute(new IndexRequest("test", "type", "1").source("{\"foo\":\"bar\"}"));
        assertTrue(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                return indexShard.getOperationCounter() == 2;
            }
        }));
        //TODO: should we wait here a little and check again?
        delayedTransportIndexAction.beginIndexLatch.countDown();
        indexResponse.get();
        assertTrue(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                return indexShard.getOperationCounter() == 1;
            }
        }));
    }

    DelayedTransportIndexAction getDelayedTransportIndexAction() {
        return new DelayedTransportIndexAction(
                getInstanceFromNode(Settings.class),
                getInstanceFromNode(TransportService.class),
                getInstanceFromNode(ClusterService.class),
                getInstanceFromNode(IndicesService.class),
                getInstanceFromNode(ThreadPool.class),
                getInstanceFromNode(ShardStateAction.class),
                getInstanceFromNode(TransportCreateIndexAction.class),
                getInstanceFromNode(MappingUpdatedAction.class),
                getInstanceFromNode(ActionFilters.class)
        );
    }


    // delays indexing until counter has been
    public static class DelayedTransportIndexAction extends TransportIndexAction {

        CountDownLatch beginIndexLatch = new CountDownLatch(1);

        public DelayedTransportIndexAction(Settings settings, TransportService transportService, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction, TransportCreateIndexAction createIndexAction, MappingUpdatedAction mappingUpdatedAction, ActionFilters actionFilters) {
            super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction, createIndexAction, mappingUpdatedAction, actionFilters);
        }

        @Override
        protected Tuple<IndexResponse, IndexRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
            beginIndexLatch.await();
            Tuple<IndexResponse, IndexRequest> response = super.shardOperationOnPrimary(clusterState, shardRequest);
            return response;
        }
    }

    ExceptionThrowingIndexAction getExceptionThrowingIndexAction() {
        return new ExceptionThrowingIndexAction(
                getInstanceFromNode(Settings.class),
                getInstanceFromNode(TransportService.class),
                getInstanceFromNode(ClusterService.class),
                getInstanceFromNode(IndicesService.class),
                getInstanceFromNode(ThreadPool.class),
                getInstanceFromNode(ShardStateAction.class),
                getInstanceFromNode(TransportCreateIndexAction.class),
                getInstanceFromNode(MappingUpdatedAction.class),
                getInstanceFromNode(ActionFilters.class)
        );
    }


    // throws some exceptions that trigger a retry in TransportShardReplicationAction and some that don't
    public static class ExceptionThrowingIndexAction extends TransportIndexAction {


        public ExceptionThrowingIndexAction(Settings settings, TransportService transportService, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction, TransportCreateIndexAction createIndexAction, MappingUpdatedAction mappingUpdatedAction, ActionFilters actionFilters) {
            super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction, createIndexAction, mappingUpdatedAction, actionFilters);
        }

        @Override
        protected Tuple<IndexResponse, IndexRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
            int randomException = randomInt(7);
            switch (randomException) {
                case 0: {
                    throw new MapperException("");
                }
                case 1: {
                    throw new IOException();
                }
                case 2: {
                    throw new InterruptedException();
                }
                case 3: {
                    throw new IllegalIndexShardStateException(new ShardId("test", 0), IndexShardState.RECOVERING, "");
                }
                case 4: {
                    throw new IndexMissingException(new Index("test"));
                }
                case 5: {
                    throw new IndexShardMissingException(new ShardId("test", 0));
                }
                case 6: {
                    throw new NoShardAvailableActionException(new ShardId("test", 0));
                }
            }
            throw new ElasticsearchException("");
        }
    }
}
