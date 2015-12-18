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

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.local.LocalTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.cluster.TestClusterService;
import org.junit.AfterClass;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DynamicMappingDisabledTests extends ESSingleNodeTestCase {

    private static ThreadPool threadPool;
    private TestClusterService clusterService;
    private LocalTransport transport;
    private TransportService transportService;
    private IndicesService indicesService;
    private ShardStateAction shardStateAction;
    private ActionFilters actionFilters;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private AutoCreateIndex autoCreateIndex;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new ThreadPool("DynamicMappingDisabledTests");
        clusterService = new TestClusterService(threadPool);
        transport = new LocalTransport(Settings.EMPTY, threadPool, Version.CURRENT, new NamedWriteableRegistry());
        transportService = new TransportService(transport, threadPool);
        indicesService = getInstanceFromNode(IndicesService.class);
        shardStateAction = new ShardStateAction(Settings.EMPTY, clusterService, transportService, null, null);
        actionFilters = new ActionFilters(new HashSet<>());
        indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);
        autoCreateIndex = new AutoCreateIndex(Settings.EMPTY, indexNameExpressionResolver);
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testDynamicDisabled() {
        Settings noDynamicIndexing = Settings.builder()
            .put(MapperService.INDEX_MAPPER_DYNAMIC_SETTING, "false")
            .build();

        TransportIndexAction action = new TransportIndexAction(noDynamicIndexing, transportService, clusterService,
            indicesService, threadPool, shardStateAction, null, null, actionFilters, indexNameExpressionResolver,
            autoCreateIndex);

        IndexRequest request = new IndexRequest("index", "type", "1");
        request.source("foo", 3);

        action.execute(request, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                fail("Indexing request should have failed");
            }

            @Override
            public void onFailure(Throwable e) {
                assertEquals(e.getMessage(), "type[[type, trying to auto create mapping for [type] in index [index], but dynamic mapping is disabled]] missing");
            }
        });
    }
}
