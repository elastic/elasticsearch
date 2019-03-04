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

package org.elasticsearch.client.indices;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

import static org.elasticsearch.index.RandomCreateIndexGenerator.randomAlias;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class RandomCreateIndexGenerator {

    /**
     * Returns a random {@link CreateIndexRequest}.
     *
     * Randomizes the index name, the aliases, mappings and settings associated with the
     * index. When present, the mappings make no mention of types.
     */
    public static CreateIndexRequest randomCreateIndexRequest() {
        try {
            // Create a random server request, and copy its contents into the HLRC request.
            // Because client requests only accept typeless mappings, we must swap out the
            // mapping definition for one that does not contain types.
            org.elasticsearch.action.admin.indices.create.CreateIndexRequest serverRequest =
                org.elasticsearch.index.RandomCreateIndexGenerator.randomCreateIndexRequest();
            return new CreateIndexRequest(serverRequest.index())
                .settings(serverRequest.settings())
                .aliases(serverRequest.aliases())
                .mapping(randomMapping());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a random mapping, with no mention of types.
     */
    public static XContentBuilder randomMapping() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        org.elasticsearch.index.RandomCreateIndexGenerator.randomMappingFields(builder, true);
        builder.endObject();
        return builder;
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
}
