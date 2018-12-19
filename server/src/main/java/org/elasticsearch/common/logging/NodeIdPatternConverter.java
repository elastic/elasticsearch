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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.util.LazyInitializable;

import java.util.concurrent.atomic.AtomicReference;


@Plugin(category = PatternConverter.CATEGORY, name = "NodeIdPatternConverter")
@ConverterKeys({"node_id_from_plugin"})
public final class NodeIdPatternConverter extends LogEventPatternConverter implements ClusterStateListener {

    AtomicReference<String> nodeId = new AtomicReference<>();

    private static LazyInitializable<NodeIdPatternConverter, Exception> INSTANCE =
        new LazyInitializable(() -> new NodeIdPatternConverter());

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static NodeIdPatternConverter newInstance(final String[] options) {
        try {
            return INSTANCE.getOrCompute();
        } catch (Exception e) {
            return null;
        }
    }

    public NodeIdPatternConverter() {
        super("NodeName", "node_id_from_plugin");
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        toAppendTo.append(nodeId.get());
    }

    @Override
    @SuppressForbidden(reason = "sets system property for logging variable propagation")
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode localNode = event.state().getNodes().getLocalNode();
        String id = localNode.getId();
        //option 2
        boolean wasSet = nodeId.compareAndSet(null, id);

        if (wasSet) {
            //option1
            System.setProperty("node_id_sys_prop", id);
            //TODO deregister as no longer the id will change ?
        }
    }

    @Override
    public String toString() {
        return nodeId.get();
    }

}
