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

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * A command to move shards in some way.
 *
 * Commands are registered in {@link NetworkModule}.
 */
public interface AllocationCommand extends NamedWriteable, ToXContent {
    interface Parser<T extends AllocationCommand> {
        /**
         * Reads an {@link AllocationCommand} of type <code>T</code> from a {@link XContentParser}.
         * @param parser {@link XContentParser} to use
         * @return {@link AllocationCommand} read
         * @throws IOException if something happens during reading
         */
        T fromXContent(XContentParser parser) throws IOException;
    }

    /**
     * Get the name of the command
     * @return name of the command
     */
    String name();

    /**
     * Executes the command on a {@link RoutingAllocation} setup
     * @param allocation {@link RoutingAllocation} to modify
     * @throws org.elasticsearch.ElasticsearchException if something happens during reconfiguration
     */
    RerouteExplanation execute(RoutingAllocation allocation, boolean explain);

    @Override
    default String getWriteableName() {
        return name();
    }
}
