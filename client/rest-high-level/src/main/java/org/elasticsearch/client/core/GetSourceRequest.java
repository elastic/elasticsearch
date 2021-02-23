/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
