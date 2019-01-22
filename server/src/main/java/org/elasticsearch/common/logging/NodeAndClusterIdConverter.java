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

import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pattern converter to format the node_and_cluster_id variable into JSON fields <code>node.id</code> and <code>cluster.uuid</code>.
 * Keeping those two fields together assures that they will be atomically set and become visible in logs at the same time.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "NodeAndClusterIdConverter")
@ConverterKeys({"node_and_cluster_id"})
public final class NodeAndClusterIdConverter extends LogEventPatternConverter {
    private static final AtomicReference<String> nodeAndClusterId = new AtomicReference<>();

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static NodeAndClusterIdConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new NodeAndClusterIdConverter();
    }

    public NodeAndClusterIdConverter() {
        super("NodeAndClusterId", "node_and_cluster_id");
    }

    /**
     * Updates only once the clusterID and nodeId
     *
     * @param nodeId      a nodeId received from cluster state update
     * @param clusterUUID a clusterId received from cluster state update
     * @return true if the update was for the first time (successful) or false if for another calls (does not updates)
     */
    public static boolean setOnce(String nodeId, String clusterUUID) {
        return nodeAndClusterId.compareAndSet(null, formatIds(clusterUUID, nodeId));
    }

    /**
     * Formats the node.id and cluster.uuid into json fields.
     *
     * @param event - a log event is ignored in this method as it uses the nodeId and clusterId to format
     */
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (nodeAndClusterId.get() != null) {
            toAppendTo.append(nodeAndClusterId.get());
        }
        // nodeId/clusterUuid not received yet, not appending
    }

    private static String formatIds(String clusterUUID, String nodeId) {
        return String.format(Locale.ROOT, "\"cluster.uuid\": \"%s\", \"node.id\": \"%s\"", clusterUUID, nodeId);
    }
}
