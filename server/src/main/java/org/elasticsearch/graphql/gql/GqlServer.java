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
import graphql.schema.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.graphql.api.GqlApi;

import java.util.Map;
import java.util.Optional;

import org.elasticsearch.graphql.gql.schema.*;

public class GqlServer {
    private static final Logger logger = LogManager.getLogger(GqlServer.class);
    GqlApi api;
    GqlBuilder builder;
    GraphQLSchema schema;
    GraphQL graphql;

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
            .get();

        schema = builder.build();
        graphql = GraphQL.newGraphQL(schema).build();
    }

    public Map<String, Object> executeToSpecification(String query, String operationName, Map<String, Object> variables, Object ctx) {
        logger.trace("GraphQL executeToSpecification {}", query);
        ExecutionResult result = graphql.execute(
            ExecutionInput.newExecutionInput(query)
                .operationName(operationName)
                .variables(variables)
                .context(ctx)
                .build()
        );
        return result.toSpecification();
    }
}
