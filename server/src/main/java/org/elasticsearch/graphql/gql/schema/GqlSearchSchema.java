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
            .name("SearchResultShards")
            .description(String.join("\n" , ""
                , "Search shard statistics."
            ))
            .field(newFieldDefinition()
                .name("total")
                .description("Number of shards used.")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("successful")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("skipped")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("failed")
                .type(GraphQLInt))
            .build())
        .type(newObject()
            .name("SearchHits")
            .description(String.join("\n" , ""
                , "Search hit statistics."
            ))
            .field(newFieldDefinition()
                .name("total")
                .description(String.join("\n" , ""
                    , "The total number of hits for the query or null if the tracking of total hits\n "
                    , "is disabled in the request."
                ))
                .type(GraphQLInt))
            .build())
        .type(newObject()
            .name("SearchResult")
            .description(String.join("\n" , ""
                , "Result of a search query."
            ))
            .field(newFieldDefinition()
                .name("took")
                .description("Number of milliseconds it took to execute the query.")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("timedOut")
                .description("Whether query did time out.")
                .type(GraphQLBoolean))
            .field(newFieldDefinition()
                .name("hits")
                .description("Search hit statistics.")
                .type(typeRef("SearchHits")))
            .field(newFieldDefinition()
                .name("maxScore")
                .description("Max search score.")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("total")
                .description("Total number of hits.")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("shards")
                .description("Query execution shard statistics.")
                .type(typeRef("SearchResultShards")))
            .field(newFieldDefinition()
                .name("documents")
                .description("List of found documents.")
                .type(nonNull(list(nonNull(typeRef("Document"))))))
            .build())
        .queryField(newFieldDefinition()
            .name("search")
            .description("Search index for matching documents.")
            .type(typeRef("SearchResult"))
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
