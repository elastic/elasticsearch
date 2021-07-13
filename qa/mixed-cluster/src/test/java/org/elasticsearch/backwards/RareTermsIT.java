/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.backwards;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;

/**
 * Test that index enough data to trigger the creation of Cuckoo filters.
 */
public class RareTermsIT extends ESRestTestCase {

    private static final String index = "idx";

    private int indexDocs(int numDocs, int id) throws Exception  {
        final Request request = new Request("POST", "/_bulk");
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numDocs; ++i) {
            builder.append("{ \"index\" : { \"_index\" : \"" + index + "\", \"_id\": \"" + id++ + "\" } }\n");
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
        createIndex(index, settings.build());
        // We want to trigger the usage oif cuckoo filters that happen only when there are
        // more than 10k distinct values in one shard.
        final int numDocs = randomIntBetween(12000, 17000);
        int id = 1;
        // Index every value 5 times
        for (int i = 0; i < 5; i++) {
            id = indexDocs(numDocs, id);
            refreshAllIndices();
        }
        // There are no rare terms that only appear in one document
        assertNumRareTerms(1, 0);
        // All terms have a cardinality lower than 10
        assertNumRareTerms(10, numDocs);
    }

    private void assertNumRareTerms(int maxDocs, int rareTerms) throws IOException {
        final Request request = new Request("POST", index + "/_search");
        request.setJsonEntity(
            "{\"aggs\" : {\"rareTerms\" : {\"rare_terms\" : {\"field\" : \"str_value.keyword\", \"max_doc_count\" : " + maxDocs + "}}}}"
        );
        final Response response = client().performRequest(request);
        assertOK(response);
        final Object o = XContentMapValues.extractValue("aggregations.rareTerms.buckets", responseAsMap(response));
        assertThat(o, Matchers.instanceOf(List.class));
        assertThat(((List<?>) o).size(), Matchers.equalTo(rareTerms));
    }
}
