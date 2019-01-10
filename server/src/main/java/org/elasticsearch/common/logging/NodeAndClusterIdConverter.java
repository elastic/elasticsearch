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

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.LazyInitializable;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;


@Plugin(category = PatternConverter.CATEGORY, name = "NodeAndClusterIdConverter")
@ConverterKeys({"node_and_cluster_id"})
public final class NodeAndClusterIdConverter extends LogEventPatternConverter implements ClusterStateListener {

    private static LazyInitializable<NodeAndClusterIdConverter, Exception> INSTANCE =
        new LazyInitializable(() -> new NodeAndClusterIdConverter());

    private AtomicReference<String> nodeAndClusterIdsReference = new AtomicReference<>();
    private CloseableThreadLocal<String> nodeAndClusterIds = new CloseableThreadLocal();

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static NodeAndClusterIdConverter newInstance(final String[] options) {
        try {
            return INSTANCE.getOrCompute();
        } catch (Exception e) {
            return null;
        }
    }

    public NodeAndClusterIdConverter() {
        super("NodeName", "node_and_cluster_id");
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (nodeAndClusterIds.get() != null) {
            //using local value
            toAppendTo.append(nodeAndClusterIds.get());
        } else if (nodeAndClusterIdsReference.get() != null) {
            //reading a value from the listener for the first time
            toAppendTo.append(nodeAndClusterIdsReference.get());
            nodeAndClusterIds.set(nodeAndClusterIdsReference.get());
        }
        // nodeId/clusterUuid not received yet, not appending
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode localNode = event.state().getNodes().getLocalNode();
        String clusterUUID = event.state().getMetaData().clusterUUID();
        String nodeId = localNode.getId();
        boolean wasSet = nodeAndClusterIdsReference.compareAndSet(null, formatIds(clusterUUID, nodeId));

        if (wasSet) {
            LOGGER.info("received first cluster state update. Setting nodeId={} and clusterUuid={}", nodeId, clusterUUID);
        }
    }

    private static String formatIds(String clusterUUID, String nodeId) {
        return String.format(Locale.ROOT, "\"cluster.uuid\": \"%s\", \"node.id\": \"%s\", ", clusterUUID, nodeId);
    }
}

