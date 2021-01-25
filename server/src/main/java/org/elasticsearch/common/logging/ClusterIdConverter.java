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
 * Pattern converter to format the cluster_id variable into JSON fields <code>cluster.id</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "ClusterIdConverter")
@ConverterKeys({"cluster_id"})
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
        if (NodeAndClusterIdStateListener.nodeAndClusterId.get() != null) {
            toAppendTo.append(NodeAndClusterIdStateListener.nodeAndClusterId.get().v2());
        }
        // nodeId/clusterUuid not received yet, not appending
    }

}
