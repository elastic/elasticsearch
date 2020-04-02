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
package org.elasticsearch.client;

import org.apache.http.HttpHeaders;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.core.StringContains.containsString;

public class HttpCompressionIT extends ESRestHighLevelClientTestCase {

    private static final String GZIP_ENCODING = "gzip";
    private static final String SAMPLE_DOCUMENT = "{\"name\":{\"first name\":\"Steve\",\"last name\":\"Jobs\"}}";

    @Before
    public void indexDocuments() throws IOException {
        Request doc = new Request(HttpPut.METHOD_NAME, "/company/_doc/1");
        doc.setJsonEntity(SAMPLE_DOCUMENT);
        client().performRequest(doc);

        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));
    }

    public void testCompressesResponseIfRequested() throws IOException {
        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader(HttpHeaders.ACCEPT_ENCODING, GZIP_ENCODING)
            .build();

        Request request = new Request("GET", "/company/_doc/1");
        request.setOptions(requestOptions);

        Response response = client().performRequest(request);

        assertOK(response);
        assertEquals(GZIP_ENCODING, response.getHeader(HttpHeaders.CONTENT_ENCODING));

        GzipDecompressingEntity decompressingEntity = new GzipDecompressingEntity(response.getEntity());
        String uncompressedContent = EntityUtils.toString(decompressingEntity);
        assertThat(uncompressedContent, containsString(SAMPLE_DOCUMENT));
    }

    public void testUncompressedResponseByDefault() throws IOException {
        Response response = client().performRequest(new Request("GET", "/"));

        assertOK(response);
        assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));

        Request request = new Request("POST", "/company/_doc/2");
        request.setJsonEntity(SAMPLE_DOCUMENT);
        response = client().performRequest(request);

        assertOK(response);
        assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));
    }

    public void testCompressesResponseIfRequestedWhileUsingRestHighLevelClient() throws IOException {
        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader(HttpHeaders.ACCEPT_ENCODING, GZIP_ENCODING)
            .build();

        SearchRequest searchRequest = new SearchRequest("company");
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync, requestOptions);

        assertOK(searchResponse);
        assertEquals(1L, searchResponse.getHits().getTotalHits().value);
        assertEquals(SAMPLE_DOCUMENT, searchResponse.getHits().getHits()[0].getSourceAsString());
    }

}
