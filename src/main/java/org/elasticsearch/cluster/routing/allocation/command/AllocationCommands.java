/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.cluster.routing.allocation.command;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class AllocationCommands {

    private static Map<String, AllocationCommand.Factory> factories = new HashMap<String, AllocationCommand.Factory>();

    /**
     * Register a custom index meta data factory. Make sure to call it from a static block.
     */
    public static void registerFactory(String type, AllocationCommand.Factory factory) {
        factories.put(type, factory);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static <T extends AllocationCommand> AllocationCommand.Factory<T> lookupFactory(String name) {
        return factories.get(name);
    }

    @SuppressWarnings("unchecked")
    public static <T extends AllocationCommand> AllocationCommand.Factory<T> lookupFactorySafe(String name) throws ElasticSearchIllegalArgumentException {
        AllocationCommand.Factory<T> factory = factories.get(name);
        if (factory == null) {
            throw new ElasticSearchIllegalArgumentException("No allocation command factory registered for name [" + name + "]");
        }
        return factory;
    }

    static {
        registerFactory(AllocateAllocationCommand.NAME, new AllocateAllocationCommand.Factory());
        registerFactory(CancelAllocationCommand.NAME, new CancelAllocationCommand.Factory());
        registerFactory(MoveAllocationCommand.NAME, new MoveAllocationCommand.Factory());
    }

    private final List<AllocationCommand> commands = Lists.newArrayList();

    public AllocationCommands(AllocationCommand... commands) {
        if (commands != null) {
            this.commands.addAll(Arrays.asList(commands));
        }
    }

    public AllocationCommands add(AllocationCommand... commands) {
        if (commands != null) {
            this.commands.addAll(Arrays.asList(commands));
        }
        return this;
    }

    public List<AllocationCommand> commands() {
        return this.commands;
    }

    public void execute(RoutingAllocation allocation) throws ElasticSearchException {
        for (AllocationCommand command : commands) {
            command.execute(allocation);
        }
    }

    public static AllocationCommands readFrom(StreamInput in) throws IOException {
        AllocationCommands commands = new AllocationCommands();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String name = in.readString();
            commands.add(lookupFactorySafe(name).readFrom(in));
        }
        return commands;
    }

    public static void writeTo(AllocationCommands commands, StreamOutput out) throws IOException {
        out.writeVInt(commands.commands.size());
        for (AllocationCommand command : commands.commands) {
            out.writeString(command.name());
            lookupFactorySafe(command.name()).writeTo(command, out);
        }
    }

    /**
     * <pre>
     *     {
     *         "commands" : [
     *              {"allocate" : {"index" : "test", "shard" : 0, "node" : "test"}}
     *         ]
     *     }
     * </pre>
     */
    public static AllocationCommands fromXContent(XContentParser parser) throws IOException {
        AllocationCommands commands = new AllocationCommands();

        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            throw new ElasticSearchParseException("No commands");
        }
        if (token == XContentParser.Token.FIELD_NAME) {
            if (!parser.currentName().equals("commands")) {
                throw new ElasticSearchParseException("expected field name to be named `commands`, got " + parser.currentName());
            }
            if (!parser.currentName().equals("commands")) {
                throw new ElasticSearchParseException("expected field name to be named `commands`, got " + parser.currentName());
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.START_ARRAY) {
                throw new ElasticSearchParseException("commands should follow with an array element");
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            // ok...
        } else {
            throw new ElasticSearchParseException("expected either field name commands, or start array, got " + token);
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                // move to the command name
                token = parser.nextToken();
                String commandName = parser.currentName();
                token = parser.nextToken();
                commands.add(AllocationCommands.lookupFactorySafe(commandName).fromXContent(parser));
                // move to the end object one
                if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    throw new ElasticSearchParseException("allocation command is malformed, done parsing a command, but didn't get END_OBJECT, got " + token);
                }
            } else {
                throw new ElasticSearchParseException("allocation command is malformed, got token " + token);
            }
        }
        return commands;
    }

    public static void toXContent(AllocationCommands commands, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("commands");
        for (AllocationCommand command : commands.commands) {
            builder.startObject();
            builder.field(command.name());
            AllocationCommands.lookupFactorySafe(command.name()).toXContent(command, builder, params);
            builder.endObject();
        }
        builder.endArray();
    }
}
