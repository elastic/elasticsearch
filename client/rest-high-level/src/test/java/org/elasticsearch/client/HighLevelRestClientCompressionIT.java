/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class HighLevelRestClientCompressionIT extends ESRestHighLevelClientTestCase {

    private static final String GZIP_ENCODING = "gzip";
    private static final String SAMPLE_DOCUMENT = "{\"name\":{\"first name\":\"Steve\",\"last name\":\"Jobs\"}}";

    public void testCompressesResponseIfRequested() throws IOException {
        Request doc = new Request(HttpPut.METHOD_NAME, "/company/_doc/1");
        doc.setJsonEntity(SAMPLE_DOCUMENT);
        client().performRequest(doc);
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));

        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader(HttpHeaders.ACCEPT_ENCODING, GZIP_ENCODING)
            .build();

        SearchRequest searchRequest = new SearchRequest("company");
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync, requestOptions);

        assertThat(searchResponse.status().getStatus(), equalTo(200));
        assertEquals(1L, searchResponse.getHits().getTotalHits().value);
        assertEquals(SAMPLE_DOCUMENT, searchResponse.getHits().getHits()[0].getSourceAsString());
    }

}
