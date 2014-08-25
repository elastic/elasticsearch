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

import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.Nullable;

import java.util.Locale;

public class DeallocationFailedException extends Exception {

    private final static String TMPL = "Failed to move shard '%d' (%s) of index '%s' from node '%s'";
    private final static String TMPL_SIMPLE = "Failed to deallocate node '%s'";


    public DeallocationFailedException(String reason) {
        super(reason);
    }

    public DeallocationFailedException(RoutingNode node) {
        super(String.format(Locale.ENGLISH, TMPL_SIMPLE, node.nodeId()));
    }

    public DeallocationFailedException(RoutingNode node, MutableShardRouting shard) {
        this(node, shard, null);
    }

    public DeallocationFailedException(RoutingNode node,
                                       MutableShardRouting shard,
                                       @Nullable String reason) {
        super(String.format(Locale.ENGLISH, TMPL,
                shard.id(),
                shard.primary() ? "primary" : "replica",
                shard.index(),
                node.nodeId()) + (reason == null ? "" : ": " + reason));
    }

    public DeallocationFailedException(RoutingNode node,
                                       MutableShardRouting shard,
                                       Throwable cause,
                                       @Nullable String reason) {
        super(String.format(Locale.ENGLISH, TMPL,
                shard.id(),
                shard.primary() ? "primary" : "replica",
                shard.index(),
                node.nodeId()) + (reason == null ? "" : ": " + reason), cause);
    }
}
