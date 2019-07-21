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
package org.elasticsearch.graphql;

import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.StaticDataFetcher;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.TypeDefinitionRegistry;
import static graphql.Scalars.*;
import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;

public class GraphqlServer {
    TypeDefinitionRegistry types;
    RuntimeWiring wiring;
    GraphQL graphql;

    public GraphqlServer() {
        types = new TypeDefinitionRegistry();

        types.add(
            newObject()
                .name("Query")
                .description("Main resolver for reading data.")
                .field(
                    newFieldDefinition()
                        .name("hello")
                        .type(GraphQLString)
                )
                .build()
                .getDefinition()
        );

        wiring = newRuntimeWiring()
            .type("Query",
                builder -> builder
                    .dataFetcher("hello", new StaticDataFetcher("world!"))
            )
            .build();

        SchemaGenerator schemaGenerator = new SchemaGenerator();
        GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(types, wiring);

        graphql = GraphQL.newGraphQL(graphQLSchema).build();
    }
}
