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

import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.util.Objects;

/**
 * High level REST client that wraps an instance of the low level {@link RestClient} and allows to build requests and read responses.
 * The provided {@link RestClient} is externally built and closed.
 */
public final class RestHighLevelClient {

    private final RestClient client;

    public RestHighLevelClient(RestClient client) {
        this.client = Objects.requireNonNull(client);
    }

    public SearchResponse performSearchRequest(SearchRequest request) throws IOException {
        StringEntity entity = new StringEntity(request.searchSource().toString());
        return new SearchResponse(this.client.performRequest("GET", buildSearchEndpoint(request), request.urlParams, entity));
    }

    public void performSearchRequestAsync(SearchRequest request, ResponseListener responseListener) throws IOException {
        StringEntity entity = new StringEntity(request.searchSource().toString());
        this.client.performRequestAsync("GET", buildSearchEndpoint(request), request.urlParams, entity, responseListener);
    }

    private static String buildSearchEndpoint(SearchRequest request) {
        String indices = String.join(",", request.indices());
        if (indices.length() > 0) {
            indices = indices + "/";
        }
        String types = String.join(",", request.types());
        if (types.length() > 0) {
            indices = indices + "/";
        }
        return "/" + indices + types + "_search";
    }
}
