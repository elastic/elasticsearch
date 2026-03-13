/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.store.Store;

import java.io.IOException;

import static org.hamcrest.Matchers.greaterThan;

public class SearchBytesReadHeaderIT extends HttpSmokeTestCase {

    public void testSearchResponseContainsBytesReadHeader() throws IOException {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());

        String indexName = "test-bytes-read-header";
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setSource("field", "value" + i).get();
        }
        indicesAdmin().prepareRefresh(indexName).get();

        long bytesRead;
        {
            Request searchRequest = new Request("POST", "/" + indexName + "/_search");
            searchRequest.setJsonEntity("""
                {
                    "query": {
                        "match_all": {}
                    },
                    "size": 1,
                    "sort" : [ { "field.keyword": "desc" } ]
                }
                """);

            Response response = getRestClient().performRequest(searchRequest);
            assertOK(response);

            String bytesReadHeader = response.getHeader("X-Elasticsearch-Bytes-Read");
            assertNotNull(bytesReadHeader);
            bytesRead = Long.parseLong(bytesReadHeader);
            assertThat(bytesRead, greaterThan(0L));
        }

        {
            Request searchRequest = new Request("POST", "/" + indexName + "/_search");
            searchRequest.setJsonEntity("""
                {
                    "query": {
                        "match_all": {}
                    },
                    "size": 2,
                    "sort" : [ { "field.keyword": "desc" } ]
                }
                """);

            Response response = getRestClient().performRequest(searchRequest);
            assertOK(response);

            String bytesReadHeader = response.getHeader("X-Elasticsearch-Bytes-Read");
            assertNotNull(bytesReadHeader);
            long bytesRead2 = Long.parseLong(bytesReadHeader);
            assertThat(bytesRead2, greaterThan(bytesRead));
        }
    }
}
