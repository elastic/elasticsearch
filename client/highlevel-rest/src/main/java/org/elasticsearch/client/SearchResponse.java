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

import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SearchResponse {

    private final XContentAccessor object;

    public SearchResponse(Response response) throws IOException {
        String contentType = response.getHeader("Content-Type");
        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(contentType);
        this.object = XContentAccessor.createFromXContent(xContentType.xContent(), body);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMap() {
        return (Map<String, Object>) this.object.getObject();
    }

    public Object get(String path) {
        return object.evaluate(path);
    }

    /**
     * Has the search operation timed out.
     */
    public boolean isTimedOut() {
        return (Boolean) get("timed_out");
    }

    /**
     * How long the search took in milliseconds.
     */
    public long getTookInMillis() {
        return this.object.evaluateLong("took");
    }

    /**
     * The total number of shards the search was executed on.
     */
    public int getTotalShards() {
        return this.object.evaluateInteger("_shards.total");
    }

    /**
     * The successful number of shards the search was executed on.
     */
    public int getSuccessfulShards() {
        return this.object.evaluateInteger("_shards.successful");
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int getFailedShards() {
        return this.object.evaluateInteger("_shards.failed");
    }

    /**
     * The search hits.
     */
    @SuppressWarnings("unchecked")
    public ClientSearchHits getHits() {
        return new ClientSearchHits((Map<String, Object>) this.object.evaluate("hits"));
    }
}
