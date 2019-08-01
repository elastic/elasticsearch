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
import org.elasticsearch.graphql.api.GqlApiUtils;
import org.elasticsearch.graphql.api.resolver.ResolverGetDocument;
import org.elasticsearch.graphql.gql.GqlBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
            .field(newFieldDefinition()
                .name("maxScore")
                .description("Max search score.")
                .type(GraphQLFloat))
            .field(newFieldDefinition()
                .name("documents")
                .description("List of found documents.")
                .type(nonNull(list(nonNull(typeRef("Document"))))))
//                .type(nonNull(list(nonNull(ExtendedScalars.Json)))))
            .build())
        .type(newObject()
            .name("SearchResult")
            .description(String.join("\n" , ""
                , "Result of a search query."
            ))
            .field(newFieldDefinition()
                .name("tookMs")
                .description("Number of milliseconds it took to execute the query.")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("timedOut")
                .description("Whether query did time out.")
                .type(GraphQLBoolean))
            .field(newFieldDefinition()
                .name("totalShards")
                .description("The total number of shards the search was executed on.")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("successfulShards")
                .description("The successful number of shards the search was executed on.")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("skippedShards")
                .description("The number of shards skipped due to pre-filtering.")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("failedShards")
                .description("The failed number of shards the search was executed on.")
                .type(GraphQLInt))
            .field(newFieldDefinition()
                .name("hits")
                .description("Search hit statistics.")
                .type(typeRef("SearchHits")))
            .build())
        .queryField(newFieldDefinition()
            .name("search")
            .description("Search index for matching documents.")
            .type(typeRef("SearchResult"))
            .argument(newArgument()
                .name("index")
                .type(GraphQLID)
                .description("Index name from which to fetch document."))
            .argument(newArgument()
                .name("indices")
                .type(list(nonNull(GraphQLID)))
                .description("Index names from which to fetch document."))
            .argument(newArgument()
                .name("q")
                .type(nonNull(GraphQLString))
                .description("Elasticsearch query string to use for matching.")))
        .fetcher("Query", "search", environment -> {
            String index = environment.getArgument("index");
            List<String> indices = environment.getArgument("indices");

            if (indices == null) {
                indices = new ArrayList<>();
            }

            if (index != null) {
                indices.add(index);
            }

            String q = environment.getArgument("q");
            String[] indexArray = {};
            indexArray = indices.toArray(indexArray);
            return api.search(indexArray, q);
        })
        .fetcher("SearchHits", "documents", environment -> {
            SearchHits hits = environment.getSource();
            List<Map<String, Object>> list = new LinkedList<Map<String, Object>>();

            if (hits == null) {
                return list;
            }

            for (SearchHit hit : hits.getHits()) {
                Map<String, Object> document = GqlApiUtils.toMap(hit);
                document = ResolverGetDocument.transformDocumentData(document);
                list.add(document);
            }

            return list;
        });
}
