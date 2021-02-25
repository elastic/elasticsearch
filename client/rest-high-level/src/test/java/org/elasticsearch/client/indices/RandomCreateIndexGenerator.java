/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
