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

package org.elasticsearch.index;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public final class RandomCreateIndexGenerator {

    private RandomCreateIndexGenerator() {}

    /**
     * Returns a random {@link CreateIndexRequest}.
     *
     * Randomizes the index name, the aliases, mappings and settings associated with the
     * index. If present, the mapping definition will be nested under a type name.
     */
    public static CreateIndexRequest randomCreateIndexRequest() throws IOException {
        String index = randomAlphaOfLength(5);
        CreateIndexRequest request = new CreateIndexRequest(index);
        randomAliases(request);
        if (randomBoolean()) {
            request.mapping(randomMapping(MapperService.SINGLE_MAPPING_NAME));
        }
        if (randomBoolean()) {
            request.settings(randomIndexSettings());
        }
        return request;
    }

    /**
     * Returns a {@link Settings} instance which include random values for
     * {@link org.elasticsearch.cluster.metadata.IndexMetadata#SETTING_NUMBER_OF_SHARDS} and
     * {@link org.elasticsearch.cluster.metadata.IndexMetadata#SETTING_NUMBER_OF_REPLICAS}
     */
    public static Settings randomIndexSettings() {
        Settings.Builder builder = Settings.builder();

        if (randomBoolean()) {
            int numberOfShards = randomIntBetween(1, 10);
            builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards);
        }

        if (randomBoolean()) {
            int numberOfReplicas = randomIntBetween(1, 10);
            builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas);
        }

        return builder.build();
    }

    /**
     * Creates a random mapping, with the mapping definition nested
     * under the given type name.
     */
    public static XContentBuilder randomMapping(String type) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        builder.startObject().startObject(type);

        randomMappingFields(builder, true);

        builder.endObject().endObject();
        return builder;
    }

    /**
     * Adds random mapping fields to the provided {@link XContentBuilder}
     */
    public static void randomMappingFields(XContentBuilder builder, boolean allowObjectField) throws IOException {
        builder.startObject("properties");

        int fieldsNo = randomIntBetween(0, 5);
        for (int i = 0; i < fieldsNo; i++) {
            builder.startObject(randomAlphaOfLength(5));

            if (allowObjectField && randomBoolean()) {
                randomMappingFields(builder, false);
            } else {
                builder.field("type", "text");
            }

            builder.endObject();
        }

        builder.endObject();
    }

    /**
     * Sets random aliases to the provided {@link CreateIndexRequest}
     */
    public static void randomAliases(CreateIndexRequest request) {
        int aliasesNo = randomIntBetween(0, 2);
        for (int i = 0; i < aliasesNo; i++) {
            request.alias(randomAlias());
        }
    }

    public static Alias randomAlias() {
        Alias alias = new Alias(randomAlphaOfLength(5));

        if (randomBoolean()) {
            if (randomBoolean()) {
                alias.routing(randomAlphaOfLength(5));
            } else {
                if (randomBoolean()) {
                    alias.indexRouting(randomAlphaOfLength(5));
                }
                if (randomBoolean()) {
                    alias.searchRouting(randomAlphaOfLength(5));
                }
            }
        }

        if (randomBoolean()) {
            alias.filter("{\"term\":{\"year\":2016}}");
        }

        if (randomBoolean()) {
            alias.writeIndex(randomBoolean());
        }

        return alias;
    }
}
