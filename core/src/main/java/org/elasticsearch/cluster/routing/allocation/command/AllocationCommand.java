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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * This interface defines the basic methods of commands for allocation
 */
public interface AllocationCommand {

    /**
     * Factory to create {@link AllocationCommand}s
     * @param <T> Type of {@link AllocationCommand}s created by this {@link Factory}
     */
    interface Factory<T extends AllocationCommand> {

        /**
         * Reads an {@link AllocationCommand} of type <code>T</code> from a {@link StreamInput}
         * @param in {@link StreamInput} to read the {@link AllocationCommand} from 
         * @return {@link AllocationCommand} read from the {@link StreamInput}
         * @throws IOException if something happens during reading
         */
        T readFrom(StreamInput in) throws IOException;
        
        /**
         * Writes an {@link AllocationCommand} to a {@link StreamOutput}
         * @param command {@link AllocationCommand} to write
         * @param out {@link StreamOutput} to write the {@link AllocationCommand} to
         * @throws IOException if something happens during writing the command
         */
        void writeTo(T command, StreamOutput out) throws IOException;
        
        /**
         * Reads an {@link AllocationCommand} of type <code>T</code> from a {@link XContentParser}
         * @param parser {@link XContentParser} to use
         * @return {@link AllocationCommand} read
         * @throws IOException if something happens during reading
         */
        T fromXContent(XContentParser parser) throws IOException;
        
        /**
         * Writes an {@link AllocationCommand} using an {@link XContentBuilder}
         * @param command {@link AllocationCommand} to write
         * @param builder {@link XContentBuilder} to use
         * @param params parameters to use when writing the command
         * @param objectName object the encoding should be encased in, null means a plain object
         * @throws IOException if something happens during writing the command
         */
        void toXContent(T command, XContentBuilder builder, ToXContent.Params params, @Nullable String objectName) throws IOException;
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
}
