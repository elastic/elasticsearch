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
package org.elasticsearch.test;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

/**
 * Base testcase for indices admin unit testing
 */
public abstract class ESIndicesTestCase extends ESTestCase {

    public static Settings randomSettings() {
        Settings.Builder builder = Settings.builder();

        int numberOfShards = randomIntBetween(0, 10);
        if (numberOfShards > 0) {
            builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
        }

        int numberOfReplicas = randomIntBetween(0, 10);
        if (numberOfReplicas >= 0) {
            builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
        }

        return builder.build();
    }

    public static XContentBuilder randomMapping(String type) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().startObject(type);

        randomMappingFields(builder, true);

        builder.endObject().endObject();
        return builder;
    }

    private static void randomMappingFields(XContentBuilder builder, boolean allowObjectField) throws IOException {
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

    public static Alias randomAlias() {
        Alias alias = new Alias(randomAlphaOfLength(5));

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

        if (randomBoolean()) {
            alias.filter("\"term\":{\"year\":2016}");
        }

        return alias;
    }
}
