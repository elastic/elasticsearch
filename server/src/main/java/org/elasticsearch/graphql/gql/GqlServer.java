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

package org.elasticsearch.graphql.gql;

import graphql.*;
import graphql.execution.SubscriptionExecutionStrategy;
import graphql.schema.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.graphql.api.GqlApi;
import static org.elasticsearch.graphql.api.GqlApiUtils.*;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.Optional;

import org.elasticsearch.graphql.gql.schema.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class GqlServer {
    private static final Logger logger = LogManager.getLogger(GqlServer.class);
    private GqlApi api;
    private GqlBuilder builder;
    private GraphQLSchema schema;
    private GraphQL graphql;

    public GqlServer(GqlApi api) {
        logger.info("Creating GraphQL server.");

        this.api = api;

        builder = Optional.of(new GqlBuilder())
            .map(new GqlScalars().use)
            .map(new GqlPingSchema().use)
            .map(new GqlInfoSchema(api).use)
            .map(new GqlDocumentSchema(api).use)
            .map(new GqlIndexSchema(api).use)
            .map(new GqlIndexInfoSchema(api).use)
            .map(new GqlNodeSchema(api).use)
            .map(new GqlDocumentUpdateSubscription(api).use)
            .map(new GqlDocumentDeleteSubscription(api).use)
            .map(new GqlIndexNewDocumentSubscription(api).use)
            .map(builder -> builder.directive(Directives.DeferDirective))
            .get();

        schema = builder.build();
        graphql = GraphQL.newGraphQL(schema)
            .subscriptionExecutionStrategy(new SubscriptionExecutionStrategy())
            .build();
    }

    /**
     * Execute GraphQL query and return serializable result in plain Java JSON-like Map-Lists.
     *
     * @param query GraphQL query ot execute.
     * @param operationName Optional name of the query.
     * @param variables Optional variables to provide to query executer in JSON-like Map-List format.
     * @param ctx {@link GraphQLContext} that can contain any optional data relevant for current request.
     * @return JSON-like Map-Lists ready for serialization to JSON sending back response to user.
     */
    public Map<String, Object> executeToSpecification(String query, String operationName, Map<String, Object> variables, GraphQLContext ctx) {
        return execute(query, operationName, variables, ctx).getSpecification();
    }

    public Map<String, Object> executeToSpecification(String query, String operationName, Map<String, Object> variables) {
        GraphQLContext ctx = GraphQLContext.newContext().build();
        return executeToSpecification(query, operationName, variables, ctx);
    }

    public GqlResult execute(String query, String operationName, Map<String, Object> variables) {
        GraphQLContext ctx = GraphQLContext.newContext().build();
        return execute(query, operationName, variables, ctx);
    }

    public GqlResult execute(String query, String operationName, Map<String, Object> variables, GraphQLContext ctx) {
        logger.trace("GraphQL execute {}", query);

        ExecutionResult result = graphql.execute(
            ExecutionInput.newExecutionInput(query)
                .operationName(operationName)
                .variables(variables)
                .context(ctx)
                .build());

        return new GqlResult() {
            @Override
            public Map<String, Object> getSpecification() {
                return result.toSpecification();
            }

            @Override
            public boolean hasDeferredResults() {
                return getDeferredResults() != null;
            }

            @Override
            @SuppressWarnings("unchecked")
            public Publisher<Map<String, Object>> getDeferredResults() {
                if (result == null) {
                    return null;
                }

                Map<Object, Object> extensions = result.getExtensions();
                if (extensions == null) {
                    return null;
                }

                Publisher<ExecutionResult> publisher = (Publisher<ExecutionResult>) extensions.get(GraphQL.DEFERRED_RESULTS);
                return transformPublisher(publisher, executionResult -> executionResult.toSpecification());
            }

            @Override
            public boolean isSubscription() {
                return result.getData() instanceof Publisher;
            }

            @Override
            public Publisher<Map<String, Object>> getSubscription() {
                if (isSubscription()) {
                    Publisher<ExecutionResult> results = result.getData();
                    return transformPublisher(results, executionResult -> executionResult.toSpecification());
                } else {
                    return new Publisher<Map<String, Object>>() {
                        boolean sent = false;

                        @Override
                        public void subscribe(Subscriber<? super Map<String, Object>> s) {
                            s.onSubscribe(new Subscription() {
                                @Override
                                public void request(long n) {
                                    if (!sent) {
                                        sent = true;
                                        s.onNext(result.getData());
                                    }
                                }

                                @Override
                                public void cancel() {}
                            });
                        }
                    };
                }
            }
        };
    }
}
