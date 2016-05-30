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
package org.elasticsearch.test.rest.client;

import org.elasticsearch.test.rest.Stash;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.elasticsearch.test.rest.json.JsonPath;

import java.io.IOException;

/**
 * Response obtained from a REST call
 * Supports parsing the response body as json when needed and returning specific values extracted from it
 */
public class RestResponse {

    private final HttpResponse response;
    private JsonPath parsedResponse;

    public RestResponse(HttpResponse response) {
        this.response = response;
    }

    public int getStatusCode() {
        return response.getStatusCode();
    }

    public String getReasonPhrase() {
        return response.getReasonPhrase();
    }

    /**
     * Returns the body properly parsed depending on the content type.
     * Might be a string or a json object parsed as a map.
     */
    public Object getBody() throws IOException {
        if (isJson()) {
            JsonPath parsedResponse = parsedResponse();
            if (parsedResponse == null) {
                return null;
            }
            return parsedResponse.evaluate("");
        }
        return response.getBody();
    }

    /**
     * Returns the body as a string
     */
    public String getBodyAsString() {
        return response.getBody();
    }

    public boolean isError() {
        return response.isError();
    }

    /**
     * Parses the response body as json and extracts a specific value from it (identified by the provided path)
     */
    public Object evaluate(String path) throws IOException {
        return evaluate(path, Stash.EMPTY);
    }

    /**
     * Parses the response body as json and extracts a specific value from it (identified by the provided path)
     */
    public Object evaluate(String path, Stash stash) throws IOException {

        if (response == null) {
            return null;
        }

        JsonPath jsonPath = parsedResponse();

        if (jsonPath == null) {
            //special case: api that don't support body (e.g. exists) return true if 200, false if 404, even if no body
            //is_true: '' means the response had no body but the client returned true (caused by 200)
            //is_false: '' means the response had no body but the client returned false (caused by 404)
            if ("".equals(path) && !response.supportsBody()) {
                return !response.isError();
            }
            return null;
        }

        return jsonPath.evaluate(path, stash);
    }

    private boolean isJson() {
        String contentType = response.getHeaders().get("Content-Type");
        return contentType != null && contentType.contains("application/json");
    }

    private JsonPath parsedResponse() throws IOException {
        if (parsedResponse != null) {
            return parsedResponse;
        }
        if (response == null || !response.hasBody()) {
            return null;
        }
        return parsedResponse = new JsonPath(response.getBody());
    }
}
