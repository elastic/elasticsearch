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

/**
 * Pattern converter to format the node_id variable into JSON fields <code>node.id</code> .
 */
@Plugin(category = PatternConverter.CATEGORY, name = "NodeIdConverter")
@ConverterKeys({ "node_id" })
public final class NodeIdConverter extends LogEventPatternConverter {
    /**
     * Called by log4j2 to initialize this converter.
     */
    public static NodeIdConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new NodeIdConverter();
    }

    public NodeIdConverter() {
        super("node_id", "node_id");
    }

    /**
     * Formats the node.id into json fields.
     *
     * @param event - a log event is ignored in this method as it uses the clusterId value
     *              from <code>NodeAndClusterIdStateListener</code> to format
     */
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (NodeAndClusterIdStateListener.nodeAndClusterId.get() != null) {
            toAppendTo.append(NodeAndClusterIdStateListener.nodeAndClusterId.get().v1());
        }
        // nodeId/clusterUuid not received yet, not appending
    }
}
