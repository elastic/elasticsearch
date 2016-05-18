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

import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * Exception thrown when an elasticsearch node responds to a request with a status code that indicates an error
 */
public class ElasticsearchResponseException extends IOException {

    private ElasticsearchResponse elasticsearchResponse;
    private final String responseBody;

    public ElasticsearchResponseException(ElasticsearchResponse elasticsearchResponse) throws IOException {
        super(buildMessage(elasticsearchResponse.getRequestLine(), elasticsearchResponse.getHost(), elasticsearchResponse.getStatusLine()));
        this.elasticsearchResponse = elasticsearchResponse;
        try {
            if (elasticsearchResponse.getEntity() == null) {
                this.responseBody = null;
            } else {
                this.responseBody = EntityUtils.toString(elasticsearchResponse.getEntity());
            }
        } finally {
            elasticsearchResponse.close();
        }
    }

    private static String buildMessage(RequestLine requestLine, HttpHost host, StatusLine statusLine) {
        return requestLine.getMethod() + " " + host + requestLine.getUri() + ": " + statusLine.toString();
    }

    /**
     * Returns the {@link ElasticsearchResponse} that caused this exception to be thrown
     */
    public ElasticsearchResponse getElasticsearchResponse() {
        return elasticsearchResponse;
    }

    /**
     * Returns the response body as a string or null if there wasn't any.
     * The body is eagerly consumed when an ElasticsearchResponseException gets created, and its corresponding ElasticsearchResponse
     * gets closed straightaway so this method is the only way to get back the response body that was returned.
     */
    public String getResponseBody() {
        return responseBody;
    }
}
