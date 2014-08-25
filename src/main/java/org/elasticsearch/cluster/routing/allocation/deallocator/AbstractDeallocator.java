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

import com.google.common.base.Joiner;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.Locale;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractDeallocator extends AbstractComponent implements Deallocator {

    static final String EXCLUDE_NODE_ID_FROM_INDEX = "index.routing.allocation.exclude._id";
    static final String CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID = "cluster.routing.allocation.exclude._id";
    static final Joiner COMMA_JOINER = Joiner.on(',');
    static final String[] EMPTY_STRING_ARRAY = new String[0];

    /**
     * executor with only 1 Thread, ensuring linearized execution of
     * requests changing cluster state
     */
    public static class ClusterChangeExecutor implements Closeable {
        private ExecutorService executor;

        public ClusterChangeExecutor() {
            executor = Executors.newSingleThreadExecutor(EsExecutors.daemonThreadFactory("deallocator"));
        }

        public <TRequest extends ActionRequest, TResponse extends ActionResponse> void enqueue(
                final TRequest request,
                final TransportAction<TRequest, TResponse> action,
                final ActionListener<TResponse> listener) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    // execute synchronously
                    try {
                        listener.onResponse(
                                action.execute(request).actionGet()
                        );
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
            });
        }

        public <TRequest extends ActionRequest, TResponse extends ActionResponse> void enqueue(
                final TRequest requests[],
                final TransportAction<TRequest, TResponse> action,
                final ActionListener<TResponse> listener) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    for (final TRequest request : requests) {
                        // execute synchronously
                        try {
                            listener.onResponse(
                                    action.execute(request).actionGet()
                            );
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }
                }
            });
        }

        @Override
        public void close() throws IOException {
            executor.shutdown();
            try {
                executor.awaitTermination(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            executor.shutdownNow();
        }
    }

    protected final ClusterChangeExecutor clusterChangeExecutor;
    protected final ClusterService clusterService;
    protected final TransportUpdateSettingsAction updateSettingsAction;
    protected final TransportClusterUpdateSettingsAction clusterUpdateSettingsAction;
    protected final AtomicReference<String> allocationEnableSetting = new AtomicReference<>();
    protected final Settings clusterSettings;

    private String localNodeId;

    public AbstractDeallocator(ClusterService clusterService, TransportUpdateSettingsAction indicesUpdateSettingsAction,
                               TransportClusterUpdateSettingsAction clusterUpdateSettingsAction, Settings clusterSettings) {
        super(ImmutableSettings.EMPTY);
        this.clusterService = clusterService;
        this.clusterChangeExecutor = new ClusterChangeExecutor();
        this.updateSettingsAction = indicesUpdateSettingsAction;
        this.clusterUpdateSettingsAction = clusterUpdateSettingsAction;
        this.clusterSettings = clusterSettings;
    }

    public String localNodeId() {
        if (localNodeId == null) {
            localNodeId = clusterService.localNode().id();
        }
        return localNodeId;
    }


    protected void setAllocationEnableSetting(final String value) {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.transientSettings(ImmutableSettings.builder().put(
                EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE,
                value));
        clusterChangeExecutor.enqueue(request, clusterUpdateSettingsAction, new ActionListener<ClusterUpdateSettingsResponse>() {
            @Override
            public void onResponse(ClusterUpdateSettingsResponse response) {
                logger.trace("[{}] setting '{}' successfully set to {}", localNodeId(), EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, value);
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("[{}] error setting '{}'", e, localNodeId(), EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE);
            }
        });
    }

    protected void trackAllocationEnableSetting() {
        String value = clusterService.state().metaData().settings().get( /// need to be transientSettings() in our version
                EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE);
        if (value == null) value = EnableAllocationDecider.Allocation.ALL.name().toLowerCase(Locale.ENGLISH);
        allocationEnableSetting.set(value);
    }

    protected void resetAllocationEnableSetting() {
        String resetValue = allocationEnableSetting.get();
        logger.trace("reset allocation.enable to {}", resetValue);
        if (resetValue != null) {
            setAllocationEnableSetting(resetValue);
        } else {
            resetValue = clusterSettings.get(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE);
            if (resetValue == null) resetValue = EnableAllocationDecider.Allocation.ALL.name().toLowerCase(Locale.ENGLISH);
            setAllocationEnableSetting(resetValue);
        }
    }

    @Override
    public void close() throws IOException {
        clusterChangeExecutor.close();
    }
}
