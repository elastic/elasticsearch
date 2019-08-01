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
import org.elasticsearch.graphql.api.GqlApi;
import org.elasticsearch.graphql.gql.GqlBuilder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Map;
import java.util.function.Function;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLID;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLTypeReference.typeRef;

public class GqlDocumentDeleteSubscription {
    GqlApi api;

    public GqlDocumentDeleteSubscription(GqlApi api) {
        this.api = api;
    }

    /**
     * - Adds `Subscription.documentDelete(index, id): Boolean` resolver.
     */
    public Function<GqlBuilder, GqlBuilder> use = builder -> builder
        .subscriptionField(newFieldDefinition()
            .name("documentDelete")
            .description("Subscription fired when document is deleted.")
            .type(GraphQLBoolean)
            .argument(newArgument()
                .name("index")
                .type(nonNull(GraphQLID))
                .description("Index name from which to fetch document."))
            .argument(newArgument()
                .name("id")
                .type(nonNull(GraphQLID))
                .description("Document ID.")))
        .fetcher("Subscription", "documentDelete", new DataFetcher() {
            @Override
            public Publisher<Boolean> get(DataFetchingEnvironment environment) {
                String indexName = environment.getArgument("index");
                String documentId = environment.getArgument("id");

                return subscriber -> {
                    api.subscribe("delete:" + indexName + ":" + documentId, new Subscriber<Object>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            subscriber.onSubscribe(s);
                        }

                        @Override
                        public void onNext(Object o) {
                            subscriber.onNext(true);
                            subscriber.onComplete();
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
