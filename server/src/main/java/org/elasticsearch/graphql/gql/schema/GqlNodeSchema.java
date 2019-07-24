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

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
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
            .field(newFieldDefinition()
                .name("_")
                .description("Fetch all `Node` data.")
                .type(ExtendedScalars.Json))
            .field(newFieldDefinition()
                .name("name")
                .description(".")
                .type(GraphQLID))
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
            String documentId = environment.getArgument("id");
            return CompletableFuture.completedFuture(new HashMap());
        })
        .fetcher("Node", "_", environment -> environment.getSource());
}
