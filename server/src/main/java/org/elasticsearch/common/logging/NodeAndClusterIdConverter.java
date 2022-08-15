/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;

import java.util.Locale;

/**
 * Pattern converter to format the node_and_cluster_id variable into JSON fields <code>node.id</code> and <code>cluster.uuid</code>.
 * Keeping those two fields together assures that they will be atomically set and become visible in logs at the same time.
 *
 * @deprecated this class is kept in order to allow working log configuration from 7.x
 */
@Plugin(category = PatternConverter.CATEGORY, name = "NodeAndClusterIdConverter")
@ConverterKeys({ "node_and_cluster_id" })
@Deprecated
public final class NodeAndClusterIdConverter extends LogEventPatternConverter {

    public NodeAndClusterIdConverter() {
        super("NodeAndClusterId", "node_and_cluster_id");
    }

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static NodeAndClusterIdConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new NodeAndClusterIdConverter();
    }

    /**
     * Formats the node.id and cluster.uuid into json fields.
     *
     * @param event - a log event is ignored in this method as it uses the nodeId and clusterId to format
     */
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (NodeAndClusterIdStateListener.nodeAndClusterId.get() != null) {
            String nodeId = NodeAndClusterIdStateListener.nodeAndClusterId.get().v1();
            String clusterUUID = NodeAndClusterIdStateListener.nodeAndClusterId.get().v2();
            toAppendTo.append(formatIds(nodeId, clusterUUID));
        }
        // nodeId/clusterUuid not received yet, not appending
    }

    private static String formatIds(String nodeId, String clusterUUID) {
        return String.format(Locale.ROOT, "\"cluster.uuid\": \"%s\", \"node.id\": \"%s\"", clusterUUID, nodeId);
    }
}
