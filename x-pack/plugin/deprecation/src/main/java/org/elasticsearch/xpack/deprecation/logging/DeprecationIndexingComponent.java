/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.function.Consumer;

import static org.elasticsearch.common.logging.DeprecationLogger.DEPRECATION_ONLY;

public class DeprecationIndexingComponent extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(DeprecationIndexingComponent.class);

    public static final Setting<Boolean> WRITE_DEPRECATION_LOGS_TO_INDEX = Setting.boolSetting(
        "cluster.deprecation_indexing.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final DeprecationIndexingAppender appender;

    public DeprecationIndexingComponent(ClusterService clusterService, ThreadPool threadPool, Client client) {
        this.appender = new DeprecationIndexingAppender(
            buildIndexRequestConsumer(threadPool, client),
            "DeprecationIndexer",
            DEPRECATION_ONLY
        );

        clusterService.addListener(this);
    }

    @Override
    protected void doStart() {
        Loggers.addAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.appender);
    }

    @Override
    protected void doStop() {
        Loggers.addAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.appender);
    }

    @Override
    protected void doClose() {
        // Nothing to do at present
    }

    /**
     * Listens for changes to the cluster state, in order to know whether to toggle indexing
     * and to set the cluster UUID and node ID. These can't be set in the constructor because
     * the initial cluster state won't be set yet.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        appender.setEnabled(WRITE_DEPRECATION_LOGS_TO_INDEX.get(state.getMetadata().settings()));
        appender.setClusterUUID(state.getMetadata().clusterUUID());
        appender.setNodeId(state.nodes().getLocalNodeId());
    }

    /**
     * Constructs a {@link Consumer} that knows what to do with the {@link IndexRequest} instances that the
     * {@link DeprecationIndexingAppender} creates. This logic is separated from the service in order to make
     * testing significantly easier, and to separate concerns.
     * <p>
     * Writes are done via {@link BulkProcessor}, which handles batching up writes and retries.
     *
     * @param threadPool due to <a href="https://github.com/elastic/elasticsearch/issues/50440">#50440</a>,
     *                   extra care must be taken to avoid blocking the thread that writes a deprecation message.
     * @param client     the client to pass to {@link BulkProcessor}
     * @return           a consumer that accepts an index request and handles all the details of writing it
     *                   into the cluster
     */
    private Consumer<IndexRequest> buildIndexRequestConsumer(ThreadPool threadPool, Client client) {
        final OriginSettingClient originSettingClient = new OriginSettingClient(client, ClientHelper.DEPRECATION_ORIGIN);

        final BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("Bulk write of deprecation logs failed: " + failure.getMessage(), failure);
            }
        };

        BulkProcessor processor = BulkProcessor.builder(originSettingClient::bulk, listener)
            .setBulkActions(100)
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .build();

        return indexRequest -> {
            try {
                // TODO: remove the threadpool wrapping when the .add call is non-blocking
                // (it can currently execute the bulk request occasionally)
                // see: https://github.com/elastic/elasticsearch/issues/50440
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> processor.add(indexRequest));
            } catch (Exception e) {
                logger.error("Failed to queue deprecation message index request: " + e.getMessage(), e);
            }
        };
    }
}
