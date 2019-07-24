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

import java.util.Map;
import java.util.function.Function;

import static graphql.Scalars.*;
import static graphql.Scalars.GraphQLID;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLTypeReference.typeRef;

public class GqlIndexSchema {
    GqlApi api;

    public GqlIndexSchema(GqlApi api) {
        this.api = api;
    }

    /**
     * - Adds `Index` GrapqhQL type.
     * - Adds `Query.index(name: ID!): Index` resolver.
     * - Adds `Index.document(documentId: ID!): Document` resolver.
     */
    @SuppressWarnings("unchecked")
    public Function<GqlBuilder, GqlBuilder> use = builder -> builder
        .type(newObject()
            .name("Index")
            .description("Elasticsearch index.")
            .field(newFieldDefinition()
                .name("_")
                .description("Fetch all `Index` data.")
                .type(ExtendedScalars.Json))
            .field(newFieldDefinition()
                .name("name")
                .description("Index name.")
                .type(nonNull(GraphQLID)))
            .field(newFieldDefinition()
                .name("numberOfShards")
                .description("Number of shard in this index.")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("autoExpandReplicas")
                .description("...")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("providerName")
                .description("...")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("format")
                .description("...")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("creationDate")
                .description("...")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("analysis")
                .description("...")
                .type(ExtendedScalars.Json))
            .field(newFieldDefinition()
                .name("priority")
                .description("...")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("numberOfReplicas")
                .description("...")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("uuid")
                .description("Unique ID of index.")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("version")
                .description("...")
                .type(ExtendedScalars.Json))
            .field(newFieldDefinition()
                .name("mappings")
                .description("Index schema mappings.")
                .type(nonNull(ExtendedScalars.Json)))
            .field(newFieldDefinition()
                .name("document")
                .description("Fetch a document from this index.")
                .type(typeRef("Document"))
                .argument(newArgument()
                    .name("id")
                    .description("Document ID.")
                    .type(nonNull(GraphQLID))))
            .build())
        .queryField(newFieldDefinition()
            .name("index")
            .description("Get index information.")
            .type(typeRef("Index"))
            .argument(newArgument()
                .name("name")
                .description("Index name")
                .type(nonNull(GraphQLID))))
        .fetcher("Query", "index", environment -> {
            String name = environment.getArgument("name");
            return api.getIndex(name);
        })
        .fetcher("Index", "_", environment -> environment.getSource())
        .fetcher("Index", "document", environment -> {
            String indexName = ((Map<String, String>) environment.getSource()).get("name");
            String documentId = environment.getArgument("id");
            return api.getDocument(indexName, documentId);
        });
}
