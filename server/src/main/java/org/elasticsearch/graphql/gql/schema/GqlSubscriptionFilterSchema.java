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

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.graphql.api.GqlApi;
import org.elasticsearch.graphql.api.GqlApiUtils;
import org.elasticsearch.graphql.api.resolver.ResolverGetDocument;
import org.elasticsearch.graphql.gql.GqlBuilder;
import org.elasticsearch.search.SearchHit;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Map;
import java.util.function.Function;

import static graphql.Scalars.*;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLTypeReference.typeRef;

public class GqlSubscriptionFilterSchema {
    GqlApi api;

    public GqlSubscriptionFilterSchema(GqlApi api) {
        this.api = api;
    }

    /**
     * - Adds `Subscription.filter(index: String, indices: [String], q: String): Document` resolver.
     */
    public Function<GqlBuilder, GqlBuilder> use = builder -> builder
        .subscriptionField(newFieldDefinition()
            .name("filter")
            .description("Filter all new documents by query field.")
            .type(typeRef("Document"))
            .argument(newArgument()
                .name("index")
                .type(GraphQLID)
                .description("Index name from which to filter new documents."))
            .argument(newArgument()
                .name("q")
                .type(nonNull(GraphQLString))
                .description("Elasticsearch query string to use for matching.")))
        .fetcher("Subscription", "filter", new DataFetcher() {
            @Override
            public Publisher<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
                String indexName = environment.getArgument("index");
                String q = environment.getArgument("q");
                String[] indices = { indexName };

                if (q.length() < 1) {
                    throw new Exception("No query provided.");
                }

                return subscriber -> {
                    api.subscribe("new_doc:" + indexName, new Subscriber<IndexResponse>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            subscriber.onSubscribe(s);
                        }

                        @Override
                        public void onNext(IndexResponse response) {
                            try {
                                String filterQuery = "(_id:" + response.getId() + ") AND " + q;
                                Thread.sleep(1000); // Wait until shard update.
                                api.search(indices, filterQuery)
                                    .thenApply(searchResponse -> {
                                        if (searchResponse.getHits().getTotal() < 1) return null;
                                        try {
                                            SearchHit hit = searchResponse.getHits().getAt(0);
                                            Map<String, Object> document = GqlApiUtils.toMap(hit);
                                            document = ResolverGetDocument.transformDocumentData(document);
                                            subscriber.onNext(document);
                                        } catch (Exception e) {
                                            System.out.println("Filter sub error: " + e);
                                            e.printStackTrace();
                                        }

                                        return null;
                                    });
                            } catch (Exception e) {
                                subscriber.onError(e);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            subscriber.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            subscriber.onComplete();
                        }
                    });
                };
            }
        });
}
