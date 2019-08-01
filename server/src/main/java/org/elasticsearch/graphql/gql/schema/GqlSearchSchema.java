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
import static graphql.schema.GraphQLList.list;

public class GqlSearchSchema {
    GqlApi api;

    public GqlSearchSchema(GqlApi api) {
        this.api = api;
    }

    /**
     * - Creates `SearchResult` GraphQL type that represents an Elasticsearch search result.
     * - Adds `Query.search(index, q): SearchResult` resolver.
     */
    public Function<GqlBuilder, GqlBuilder> use = builder -> builder
        .type(newObject()
            .name("SearchResult")
            .description(String.join("\n" , ""
                , "Result of a search query."
            ))
            .field(newFieldDefinition()
                .name("hits")
                .description("Total number of documents found.")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("documents")
                .description("List of found documents.")
                .type(nonNull(list(nonNull(typeRef("Document"))))))
            .build())
        .queryField(newFieldDefinition()
            .name("search")
            .description("Search index for matching documents.")
            .type(ExtendedScalars.Json)
            .argument(newArgument()
                .name("index")
                .type(nonNull(GraphQLID))
                .description("Index name from which to fetch document."))
            .argument(newArgument()
                .name("q")
                .type(nonNull(GraphQLString))
                .description("Elasticsearch query string to use for matching.")))
        .fetcher("Query", "search", environment -> {
            String indexName = environment.getArgument("index");
            String q = environment.getArgument("q");
            return api.search(indexName, q);
        });
}
