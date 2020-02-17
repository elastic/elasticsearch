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

package org.elasticsearch.client.core;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

public final class GetSourceRequest implements Validatable {
    private String routing;
    private String preference;

    private boolean refresh = false;
    private boolean realtime = true;

    private FetchSourceContext fetchSourceContext;

    private final String index;
    private final String id;

    public GetSourceRequest(String index, String id) {
        this.index = index;
        this.id = id;
    }

    public static GetSourceRequest from(GetRequest getRequest) {
        return new GetSourceRequest(getRequest.index(), getRequest.id())
            .routing(getRequest.routing())
            .preference(getRequest.preference())
            .refresh(getRequest.refresh())
            .realtime(getRequest.realtime())
            .fetchSourceContext(getRequest.fetchSourceContext());
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public GetSourceRequest routing(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public GetSourceRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    /**
     * Should a refresh be executed before this get operation causing the operation to
     * return the latest value. Note, heavy get should not set this to {@code true}. Defaults
     * to {@code false}.
     */
    public GetSourceRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public GetSourceRequest realtime(boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    /**
     * Allows setting the {@link FetchSourceContext} for this request, controlling if and how _source should be returned.
     * Note, the {@code fetchSource} field of the context must be set to {@code true}.
     */

    public GetSourceRequest fetchSourceContext(FetchSourceContext context) {
        this.fetchSourceContext = context;
        return this;
    }

    public String index() {
        return index;
    }

    public String id() {
        return id;
    }

    public String routing() {
        return routing;
    }

    public String preference() {
        return preference;
    }

    public boolean refresh() {
        return refresh;
    }

    public boolean realtime() {
        return realtime;
    }

    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }
}
