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
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;

/**
 * Test that index enough data to trigger concurrency.
 */
public class HistogramIT extends ESRestTestCase {

    private static final String index = "idx";
    private static final int numBuckets = 1000;
    private static final int docsPerBuckets = 1000;

    private int indexDocs(int numDocs, int id) throws Exception {
        final Request request = new Request("POST", "/_bulk");
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numDocs; ++i) {
            Object[] args = new Object[] { index, id++, i * 1000 * 60, i };
            builder.append(Strings.format("""
                { "index" : { "_index" : "%s", "_id": "%s" } }
                {"date" : %s, "number" : %s }
                """, args));
        }
        request.setJsonEntity(builder.toString());
        assertOK(client().performRequest(request));
        return id;
    }

    public void testWithConcurrency() throws Exception {
        final String mapping = """
             "properties": {
               "date": { "type": "date" },
               "number": { "type": "integer" }
             }
            """;
        final Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0);
        createIndex(index, settings.build(), mapping);
        // We want to trigger concurrency so we need to index enough documents
        int id = 1;
        for (int i = 0; i < docsPerBuckets; i++) {
            id = indexDocs(numBuckets, id);
            refreshAllIndices();
        }
        // Check date histogram
        assertDateHistogram();
        // Check histogram
        assertHistogram();
    }

    private void assertDateHistogram() throws IOException {
        final Request request = new Request("POST", index + "/_search");
        request.setJsonEntity("""
            {
              "aggs": {
                "hist": {
                  "date_histogram": {
                    "field": "date",
                    "calendar_interval": "minute"
                  }
                }
              }
            }""");
        final Response response = client().performRequest(request);
        assertOK(response);
        ObjectPath o = ObjectPath.createFromResponse(response);
        assertEquals(numBuckets, o.evaluateArraySize("aggregations.hist.buckets"));
        for (int j = 0; j < numBuckets; j++) {
            assertEquals(docsPerBuckets, (int) o.evaluate("aggregations.hist.buckets." + j + ".doc_count"));
        }
    }

    private void assertHistogram() throws IOException {
        final Request request = new Request("POST", index + "/_search");
        request.setJsonEntity("""
            {
              "aggs": {
                "hist": {
                  "histogram": {
                    "field": "number",
                    "interval": "1"
                  }
                }
              }
            }""");
        final Response response = client().performRequest(request);
        assertOK(response);
        ObjectPath o = ObjectPath.createFromResponse(response);
        assertEquals(numBuckets, o.evaluateArraySize("aggregations.hist.buckets"));
        for (int j = 0; j < numBuckets; j++) {
            assertEquals(docsPerBuckets, (int) o.evaluate("aggregations.hist.buckets." + j + ".doc_count"));
        }
    }
}
