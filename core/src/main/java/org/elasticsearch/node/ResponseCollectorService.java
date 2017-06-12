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

package org.elasticsearch.node;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Collects statistics about queue size, response time, and service time of
 * tasks executed on each node, making the EWMA of the values available to the
 * coordinating node.
 */
public class ResponseCollectorService extends AbstractComponent implements ClusterStateListener {

    private static final double ALPHA = 0.3;

    private final Map<String, ExponentiallyWeightedMovingAverage> nodeIdToQueueSize;
    private final Map<String, ExponentiallyWeightedMovingAverage> nodeIdToResponseTime;
    private final Map<String, Double> nodeIdToAvgServiceTime;

    @Inject
    public ResponseCollectorService(Settings settings, ClusterService clusterService) {
        super(settings);
        this.nodeIdToQueueSize = new HashMap<>();
        this.nodeIdToResponseTime = new HashMap<>();
        this.nodeIdToAvgServiceTime = new HashMap<>();
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                synchronized (this) {
                    nodeIdToQueueSize.remove(removedNode.getId());
                    nodeIdToResponseTime.remove(removedNode.getId());
                    nodeIdToAvgServiceTime.remove(removedNode.getId());
                }
            }
        }
    }

    private void initializeOrAdd(Map<String, ExponentiallyWeightedMovingAverage> map, String key, double value) {
        ExponentiallyWeightedMovingAverage avg = map.get(key);
        if (avg == null) {
            synchronized (this) {
                avg = map.get(key);
                if (avg == null) {
                    map.put(key, new ExponentiallyWeightedMovingAverage(ALPHA, value));
                } else {
                    avg.addValue(value);
                }
            }
        } else {
            avg.addValue(value);
        }
    }

    public void addQueueSize(String nodeId, int queueSize) {
        initializeOrAdd(nodeIdToQueueSize, nodeId, (double) queueSize);
    }

    public void addResponseTime(String nodeId, long responseTimeNanos) {
        initializeOrAdd(nodeIdToResponseTime, nodeId, (double) responseTimeNanos);
    }

    public void setServiceTime(String nodeId, long avgServiceTimeNanos) {
        nodeIdToAvgServiceTime.put(nodeId, (double) avgServiceTimeNanos);
    }

    public synchronized Map<String, Double> getAvgQueueSize() {
        return nodeIdToQueueSize
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().getAverage()));
    }

    public synchronized Map<String, Double> getAvgResponseTime() {
        return nodeIdToResponseTime
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().getAverage()));
    }

    public Map<String, Double> getAvgServiceTime() {
        return Collections.unmodifiableMap(nodeIdToAvgServiceTime);
    }
}
