/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl;/*
                                       * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
                                       * or more contributor license agreements. Licensed under the Elastic License
                                       * 2.0 and the Server Side Public License, v 1; you may not use this file except
                                       * in compliance with, at your election, the Elastic License 2.0 or the Server
                                       * Side Public License, v 1.
                                       */

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.elasticsearch.logging.spi.ServerSupport;

/**
 * Pattern converter to format the cluster_id variable into JSON fields <code>cluster.id</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "org.elasticsearch.logging.impl.ClusterIdConverter")
@ConverterKeys({ "cluster_id" })
public final class ClusterIdConverter extends LogEventPatternConverter {
    /**
     * Called by log4j2 to initialize this converter.
     */
    public static ClusterIdConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new ClusterIdConverter();
    }

    public ClusterIdConverter() {
        super("cluster_id", "cluster_id");
    }

    /**
     * Formats the cluster.uuid into json fields.
     *
     * @param event - a log event is ignored in this method as it uses the clusterId value
     *              from <code>NodeAndClusterIdStateListener</code> to format
     */
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        String nodeAndClusterId = ServerSupport.INSTANCE.clusterId();
        if (nodeAndClusterId != null) {
            toAppendTo.append(nodeAndClusterId);
        }
        // nodeId/clusterUuid not received yet, not appending
    }

}
