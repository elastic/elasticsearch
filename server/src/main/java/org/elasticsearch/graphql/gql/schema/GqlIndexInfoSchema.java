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

import java.util.Map;
import java.util.function.Function;

import static graphql.Scalars.*;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLTypeReference.typeRef;

public class GqlIndexInfoSchema {
    GqlApi api;

    public GqlIndexInfoSchema(GqlApi api) {
        this.api = api;
    }

    /**
     * - Adds `IndexInfo` GrapqhQL type.
     * - Adds `Query.indices: [IndexInfo]` resolver.
     */
    @SuppressWarnings("unchecked")
    public Function<GqlBuilder, GqlBuilder> use = builder -> builder
        .type(newObject()
            .name("IndexInfo")
            .description("Elasticsearch database index information.")
            .field(newFieldDefinition()
                .name("health")
                .description("Status of Elasticsearch index.")
                .type(nonNull(GraphQLString)))
            .field(newFieldDefinition()
                .name("status")
                .description("Database index status.")
                .type(nonNull(GraphQLString)))
            .field(newFieldDefinition()
                .name("name")
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
            .field(newFieldDefinition()
                .name("index")
                .type(typeRef("Index")))
            .build())
        .queryField(newFieldDefinition()
            .name("indexInfo")
            .description("List all Elasticsearch indices.")
            .type(nonNull(list(nonNull(typeRef("IndexInfo"))))))
        .fetcher("Query", "indexInfo", environment -> api.getIndexInfos())
        .fetcher("IndexInfo", "index", environment -> {
            String indexName = (String) ((Map<String, Object>) environment.getSource()).get("name");
            return api.getIndex(indexName);
        });
}
