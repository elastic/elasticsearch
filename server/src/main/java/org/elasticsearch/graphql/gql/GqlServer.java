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

import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLTypeReference.typeRef;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.Scalars.*;

public class GqlServer {
    private static final Logger logger = LogManager.getLogger(GqlServer.class);
    GqlApi api;
    GqlBuilder builder;
    GraphQLSchema schema;
    GraphQL graphql;

    public GqlServer(GqlApi api) {
        logger.info("Creating GraphQL server.");

        this.api = api;
        builder = new GqlBuilder();

        addPingResolver(builder);
        addFooResolver(builder);
        addHelloQuery(builder);
        addIndicesQuery(builder);

        schema = builder.build();
        graphql = GraphQL.newGraphQL(schema).build();
    }

    private void addPingResolver(GqlBuilder builder) {
        builder
            .queryField(newFieldDefinition()
                .description("Ping server if it is available.")
                .name("ping")
                .type(nonNull(GraphQLString)))
            .fetcher("Query", "ping", new StaticDataFetcher("pong"));
    }

    private void addFooResolver(GqlBuilder builder) {
        builder
            .queryField(newFieldDefinition()
                .description("Sample resolver.")
                .name("foo")
                .type(nonNull(GraphQLString)))
            .fetcher("Query", "foo", new StaticDataFetcher("bar"));
    }

    private void addHelloQuery(GqlBuilder builder) {
        builder
            .type(newObject()
                .name("Version")
                .description("Server version information.")
                .field(newFieldDefinition()
                    .name("number")
                    .description("Build version.")
                    .type(GraphQLString))
                .field(newFieldDefinition()
                    .name("build_flavor")
                    .type(GraphQLString))
                    .description("...")
                .field(newFieldDefinition()
                    .name("build_type")
                    .description("...")
                    .type(GraphQLString))
                .field(newFieldDefinition()
                    .name("build_hash")
                    .description("...")
                    .type(GraphQLString))
                .field(newFieldDefinition()
                    .name("build_date")
                    .description("...")
                    .type(GraphQLString))
                .field(newFieldDefinition()
                    .name("build_snapshot")
                    .description("...")
                    .type(GraphQLString))
                .field(newFieldDefinition()
                    .name("lucene_version")
                    .description("...")
                    .type(GraphQLString))
                .field(newFieldDefinition()
                    .name("minimum_wire_compatibility_version")
                    .description("...")
                    .type(GraphQLString))
                .field(newFieldDefinition()
                    .name("minimum_index_compatibility_version")
                    .description("...")
                    .type(GraphQLString))
                .build())
            .type(newObject()
                .name("HelloInfo")
                .description("Server hello information.")
                .field(newFieldDefinition()
                    .name("name")
                    .description("Server node name.")
                    .type(GraphQLString))
                .field(newFieldDefinition()
                    .name("cluster_name")
                    .description("Name of the server cluster.")
                    .type(GraphQLString))
                .field(newFieldDefinition()
                    .name("cluster_uuid")
                    .description("UUID.")
                    .type(GraphQLString))
                .field(newFieldDefinition()
                    .name("version")
                    .description("Version information of various server components.")
                    .type(nonNull(typeRef("Version"))))
                .field(newFieldDefinition()
                    .name("tagline")
                    .description("Elasticsearch moto.")
                    .type(GraphQLString))
                .build())
            .queryField(newFieldDefinition()
                .name("hello")
                .description("Get generic server information.")
                .type(nonNull(typeRef("HelloInfo"))))
            .fetcher("Query", "hello", environment -> api.getHello());
    }

    private void addIndicesQuery(GqlBuilder builder) {
        builder
            .type(newObject()
                .name("Index")
                .description("Elasticsearch database index.")
                .field(newFieldDefinition()
                    .name("health")
                    .description("Status of Elasticsearch index.")
                    .type(nonNull(GraphQLString)))
                .field(newFieldDefinition()
                    .name("status")
                    .description("Database index status.")
                    .type(nonNull(GraphQLString)))
                .field(newFieldDefinition()
                    .name("index")
                    .description("Index name.")
                    .type(nonNull(GraphQLString)))
                .field(newFieldDefinition()
                    .name("uuid")
                    .description("Global unique ID of the index.")
                    .type(nonNull(GraphQLString)))
                .field(newFieldDefinition()
                    .name("pri")
                    .description("...")
                    .type(nonNull(GraphQLString)))
                .field(newFieldDefinition()
                    .name("rep")
                    .description("...")
                    .type(nonNull(GraphQLString)))
                .field(newFieldDefinition()
                    .name("docsCount")
                    .description("Number of documents stored in the index.")
                    .type(nonNull(GraphQLString)))
                .field(newFieldDefinition()
                    .name("docsDeleted")
                    .description("Number of deleted documents.")
                    .type(nonNull(GraphQLString)))
                .field(newFieldDefinition()
                    .name("storeSize")
                    .description("...")
                    .type(nonNull(GraphQLString)))
                .field(newFieldDefinition()
                    .name("priStoreSize")
                    .description("...")
                    .type(nonNull(GraphQLString)))
                .build())
            .queryField(newFieldDefinition()
                .name("indices")
                .description("List all Elasticsearch indices.")
                .type(nonNull(list(nonNull(typeRef("Index"))))))
            .fetcher("Query", "indices", environment -> api.getIndices());
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
