/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author kimchy (Shay Banon)
 */
public interface ShardRouting extends Streamable, Serializable {

    String index();

    int id();

    boolean unassigned();

    boolean initializing();

    boolean started();

    boolean relocating();

    /**
     * Relocating or started.
     */
    boolean active();

    boolean assignedToNode();

    String currentNodeId();

    String relocatingNodeId();

    boolean primary();

    ShardRoutingState state();

    ShardId shardId();

    String shortSummary();

    /**
     * Does not write index name and shard id
     */
    void writeToThin(DataOutput out) throws IOException;

    void readFromThin(DataInput in) throws ClassNotFoundException, IOException;
}
