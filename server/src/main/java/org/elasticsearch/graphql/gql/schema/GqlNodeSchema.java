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

package org.elasticsearch.graphql.gql.schema;

import graphql.scalars.ExtendedScalars;
import org.elasticsearch.graphql.api.GqlApi;
import org.elasticsearch.graphql.gql.GqlBuilder;

import java.util.function.Function;

import static graphql.Scalars.*;
import static graphql.Scalars.GraphQLID;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLTypeReference.typeRef;

public class GqlNodeSchema {
    GqlApi api;

    public GqlNodeSchema(GqlApi api) {
        this.api = api;
    }

    /**
     * - Creates `Node` GraphQL type that represents an Elasticsearch document.
     * - Adds `Query.node(id): Node` resolver.
     */
    public Function<GqlBuilder, GqlBuilder> use = builder -> builder
        .type(newObject()
            .name("Node")
            .description(String.join("\n" , ""
                , "`Node` represents an Elasticsearch server node."
            ))
            .field(newFieldDefinition().name("_").type(ExtendedScalars.Json)).description("Fetch all `Node` data.")
            .field(newFieldDefinition().name("name").type(GraphQLID))
            .field(newFieldDefinition().name("transportAddress").type(GraphQLString))
            .field(newFieldDefinition().name("host").type(GraphQLString))
            .field(newFieldDefinition().name("ip").type(GraphQLString))
            .field(newFieldDefinition().name("buildFlavor").type(GraphQLString))
            .field(newFieldDefinition().name("buildType").type(GraphQLString))
            .field(newFieldDefinition().name("buildHash").type(GraphQLString))
            .field(newFieldDefinition().name("totalIndexingBuffer").type(GraphQLInt))
            .field(newFieldDefinition().name("roles").type(ExtendedScalars.Json))
            .field(newFieldDefinition().name("attributes").type(ExtendedScalars.Json))
            .field(newFieldDefinition().name("settings").type(ExtendedScalars.Json))
            .field(newFieldDefinition().name("os").type(ExtendedScalars.Json))
            .field(newFieldDefinition().name("process").type(ExtendedScalars.Json))
            .field(newFieldDefinition().name("jvm").type(ExtendedScalars.Json))
            .field(newFieldDefinition().name("threadPool").type(ExtendedScalars.Json))
            .field(newFieldDefinition().name("transport").type(ExtendedScalars.Json))
            .field(newFieldDefinition().name("http").type(ExtendedScalars.Json))
            .field(newFieldDefinition().name("plugins").type(ExtendedScalars.Json))
            .field(newFieldDefinition().name("modules").type(ExtendedScalars.Json))
            .field(newFieldDefinition().name("ingest").type(ExtendedScalars.Json))
            .build())
        .queryField(newFieldDefinition()
            .name("node")
            .description("Fetches an Elasticsearch node.")
            .type(typeRef("Node"))
            .argument(newArgument()
                .name("id")
                .type(nonNull(GraphQLID))
                .description("ID or name of the node.")))
        .fetcher("Query", "node", environment -> {
            String nodeIdOrName = environment.getArgument("id");
            return api.getNode(nodeIdOrName);
        })
        .fetcher("Node", "_", environment -> environment.getSource());
}
