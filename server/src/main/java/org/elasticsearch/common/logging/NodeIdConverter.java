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

/**
 * Pattern converter to format the node_and_cluster_id variable into JSON fields <code>node.id</code> and <code>cluster.uuid</code>.
 * Keeping those two fields together assures that they will be atomically set and become visible in logs at the same time.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "NodeIdConverter")
@ConverterKeys({"ESnode_id"})
public final class NodeIdConverter extends LogEventPatternConverter {
    /**
     * Called by log4j2 to initialize this converter.
     */
    public static NodeIdConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new NodeIdConverter();
    }

    public NodeIdConverter() {
        super("ESnode_id", "ESnode_id");
    }

    /**
     * Formats the node.id and cluster.uuid into json fields.
     *
     * @param event - a log event is ignored in this method as it uses the nodeId and clusterId to format
     */
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (NodeAndClusterIdStateListener.nodeAndClusterId.get() != null) {
            toAppendTo.append(NodeAndClusterIdStateListener.nodeAndClusterId.get().v1());
        }
        // nodeId/clusterUuid not received yet, not appending
    }
}
