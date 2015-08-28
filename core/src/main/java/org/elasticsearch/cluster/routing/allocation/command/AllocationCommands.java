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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple {@link AllocationCommand} composite managing several
 * {@link AllocationCommand} implementations
 */
public class AllocationCommands {

    private static Map<String, AllocationCommand.Factory> factories = new HashMap<>();

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
    public static <T extends AllocationCommand> AllocationCommand.Factory<T> lookupFactorySafe(String name) {
        AllocationCommand.Factory<T> factory = factories.get(name);
        if (factory == null) {
            throw new IllegalArgumentException("No allocation command factory registered for name [" + name + "]");
        }
        return factory;
    }

    static {
        registerFactory(AllocateAllocationCommand.NAME, new AllocateAllocationCommand.Factory());
        registerFactory(CancelAllocationCommand.NAME, new CancelAllocationCommand.Factory());
        registerFactory(MoveAllocationCommand.NAME, new MoveAllocationCommand.Factory());
    }

    private final List<AllocationCommand> commands = new ArrayList<>();

    /**
     * Creates a new set of {@link AllocationCommands}
     *   
     * @param commands {@link AllocationCommand}s that are wrapped by this instance
     */
    public AllocationCommands(AllocationCommand... commands) {
        if (commands != null) {
            this.commands.addAll(Arrays.asList(commands));
        }
    }

    /**
     * Adds a set of commands to this collection
     * @param commands Array of commands to add to this instance
     * @return {@link AllocationCommands} with the given commands added
     */
    public AllocationCommands add(AllocationCommand... commands) {
        if (commands != null) {
            this.commands.addAll(Arrays.asList(commands));
        }
        return this;
    }

    /**
     * Get the commands wrapped by this instance
     * @return {@link List} of commands
     */
    public List<AllocationCommand> commands() {
        return this.commands;
    }

    /**
     * Executes all wrapped commands on a given {@link RoutingAllocation}
     * @param allocation {@link RoutingAllocation} to apply this command to
     * @throws org.elasticsearch.ElasticsearchException if something happens during execution
     */
    public RoutingExplanations execute(RoutingAllocation allocation, boolean explain) {
        RoutingExplanations explanations = new RoutingExplanations();
        for (AllocationCommand command : commands) {
            explanations.add(command.execute(allocation, explain));
        }
        return explanations;
    }

    /**
     * Reads a {@link AllocationCommands} from a {@link StreamInput}
     * @param in {@link StreamInput} to read from
     * @return {@link AllocationCommands} read
     * 
     * @throws IOException if something happens during read
     */
    public static AllocationCommands readFrom(StreamInput in) throws IOException {
        AllocationCommands commands = new AllocationCommands();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String name = in.readString();
            commands.add(lookupFactorySafe(name).readFrom(in));
        }
        return commands;
    }

    /**
     * Writes {@link AllocationCommands} to a {@link StreamOutput}
     * 
     * @param commands Commands to write
     * @param out {@link StreamOutput} to write the commands to
     * @throws IOException if something happens during write
     */
    public static void writeTo(AllocationCommands commands, StreamOutput out) throws IOException {
        out.writeVInt(commands.commands.size());
        for (AllocationCommand command : commands.commands) {
            out.writeString(command.name());
            lookupFactorySafe(command.name()).writeTo(command, out);
        }
    }
    
    /**
     * Reads {@link AllocationCommands} from a {@link XContentParser}
     * <pre>
     *     {
     *         "commands" : [
     *              {"allocate" : {"index" : "test", "shard" : 0, "node" : "test"}}
     *         ]
     *     }
     * </pre>
     * @param parser {@link XContentParser} to read the commands from
     * @return {@link AllocationCommands} read
     * @throws IOException if something bad happens while reading the stream 
     */
    public static AllocationCommands fromXContent(XContentParser parser) throws IOException {
        AllocationCommands commands = new AllocationCommands();

        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            throw new ElasticsearchParseException("No commands");
        }
        if (token == XContentParser.Token.FIELD_NAME) {
            if (!parser.currentName().equals("commands")) {
                throw new ElasticsearchParseException("expected field name to be named [commands], got [{}] instead", parser.currentName());
            }
            if (!parser.currentName().equals("commands")) {
                throw new ElasticsearchParseException("expected field name to be named [commands], got [{}] instead", parser.currentName());
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.START_ARRAY) {
                throw new ElasticsearchParseException("commands should follow with an array element");
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            // ok...
        } else {
            throw new ElasticsearchParseException("expected either field name [commands], or start array, got [{}] instead", token);
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
                    throw new ElasticsearchParseException("allocation command is malformed, done parsing a command, but didn't get END_OBJECT, got [{}] instead", token);
                }
            } else {
                throw new ElasticsearchParseException("allocation command is malformed, got [{}] instead", token);
            }
        }
        return commands;
    }
    
    /**
     * Writes {@link AllocationCommands} to a {@link XContentBuilder}
     * 
     * @param commands {@link AllocationCommands} to write
     * @param builder {@link XContentBuilder} to use
     * @param params Parameters to use for building
     * @throws IOException if something bad happens while building the content
     */
    public static void toXContent(AllocationCommands commands, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("commands");
        for (AllocationCommand command : commands.commands) {
            builder.startObject();
            builder.field(command.name());
            AllocationCommands.lookupFactorySafe(command.name()).toXContent(command, builder, params, null);
            builder.endObject();
        }
        builder.endArray();
    }
}
