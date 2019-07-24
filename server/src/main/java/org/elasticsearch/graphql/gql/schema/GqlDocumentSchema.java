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

public class GqlDocumentSchema {
    GqlApi api;

    public GqlDocumentSchema(GqlApi api) {
        this.api = api;
    }

    /**
     * - Creates `Document` GraphQL type that represents an Elasticsearch document.
     * - Adds `Query.document(index, id): Document` resolver.
     */
    public Function<GqlBuilder, GqlBuilder> use = builder -> builder
        .type(newObject()
            .name("Document")
            .description(String.join("\n" , ""
                    , "`Document` represents a JSON document stored in Elasticsearch."
            ))
            .field(newFieldDefinition()
                .name("_")
                .description("Fetch all `Document` data.")
                .type(ExtendedScalars.Json))
            .field(newFieldDefinition()
                .name("indexName")
                .description("Document index name.")
                .type(GraphQLID))
            .field(newFieldDefinition()
                .name("type")
                .description("Type of the document.")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("id")
                .description("ID of the document.")
                .type(GraphQLID))
            .field(newFieldDefinition()
                .name("version")
                .description("...")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("sequenceNumber")
                .description("...")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("primaryTerm")
                .description("...")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("found")
                .description("...")
                .type(GraphQLBoolean))
            .field(newFieldDefinition()
                .name("source")
                .description("Contents of the document.")
                .type(ExtendedScalars.Json))
            .build())
        .queryField(newFieldDefinition()
            .name("document")
            .description("Fetches document using document get API based on its id.")
            .type(typeRef("Document"))
            .argument(newArgument()
                .name("index")
                .type(nonNull(GraphQLID))
                .description("Index name from which to fetch document."))
            .argument(newArgument()
                .name("id")
                .type(nonNull(GraphQLID))
                .description("Document ID.")))
        .fetcher("Query", "document", environment -> {
            String indexName = environment.getArgument("index");
            String documentId = environment.getArgument("id");
            return api.getDocument(indexName, documentId);
        })
        .fetcher("Document", "_", environment -> environment.getSource());
}
