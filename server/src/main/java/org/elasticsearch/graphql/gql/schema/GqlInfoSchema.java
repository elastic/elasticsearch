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

import org.elasticsearch.graphql.api.GqlApi;
import org.elasticsearch.graphql.gql.GqlBuilder;

import java.util.function.Function;

import static graphql.Scalars.*;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLTypeReference.typeRef;

public class GqlInfoSchema {

    GqlApi api;

    public GqlInfoSchema(GqlApi api) {
        this.api = api;
    }

    /**
     * - Adds `Info` and `InfoVersion` GraphQL types.
     * - Adds `Query.info: Info` resolver.
     */
    public Function<GqlBuilder, GqlBuilder> use = builder -> builder
        .type(newObject()
            .name("InfoVersion")
            .description("Server version information.")
            .field(newFieldDefinition()
                .name("number")
                .description("Build version.")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("buildFlavor")
                .type(GraphQLString))
            .description("...")
            .field(newFieldDefinition()
                .name("buildType")
                .description("...")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("buildHash")
                .description("...")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("buildDate")
                .description("...")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("buildSnapshot")
                .description("...")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("lucene")
                .description("...")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("minimumWireCompatibilityVersion")
                .description("...")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("minimumIndexCompatibilityVersion")
                .description("...")
                .type(GraphQLString))
            .build())
        .type(newObject()
            .name("Info")
            .description("Server hello information.")
            .field(newFieldDefinition()
                .name("name")
                .description("Server node name.")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("clusterName")
                .description("Name of the server cluster.")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("clusterUuid")
                .description("UUID.")
                .type(GraphQLString))
            .field(newFieldDefinition()
                .name("version")
                .description("Version information of various server components.")
                .type(nonNull(typeRef("InfoVersion"))))
            .field(newFieldDefinition()
                .name("tagline")
                .description("Elasticsearch moto.")
                .type(GraphQLString))
            .build())
        .queryField(newFieldDefinition()
            .name("info")
            .description("Get generic server information.")
            .type(nonNull(typeRef("Info"))))
        .fetcher("Query", "info", environment -> api.getInfo());

}
