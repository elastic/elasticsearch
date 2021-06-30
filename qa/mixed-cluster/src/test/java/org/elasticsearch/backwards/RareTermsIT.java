/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.backwards;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;

/**
 * Test that index enough data to trigger the creation of Cuckoo filters.
 */
public class RareTermsIT extends ESRestTestCase {

    private int indexDocs(int numDocs, int id) throws Exception  {
        final Request request = new Request("POST", "/_bulk");
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numDocs; ++i) {
            builder.append("{ \"index\" : { \"_index\" : \"idx\", \"_id\": \"" + id++ + "\" } }\n");
            builder.append("{\"str_value\" : \"s" + i + "\"}\n");
        }
        request.setJsonEntity(builder.toString());
        assertOK(client().performRequest(request));
        return id;
    }

    public void testSingleValuedString() throws Exception {
        final Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 2)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0);
        final String index = "idx";
        createIndex(index, settings.build());

        final int numDocs = randomIntBetween(12000, 17000);
        int id = 1;
        for (int i = 0; i < 5; i++) {
            id = indexDocs(numDocs, id);
            refreshAllIndices();
        }

        final Request request = new Request("POST", "idx/_search");
        request.setJsonEntity("{\"size\": 0,\"aggs\":{\"rareTerms\":{\"rare_terms\" : {\"field\": \"str_value.keyword\"}}}}");
        assertOK(client().performRequest(request));
    }
}
