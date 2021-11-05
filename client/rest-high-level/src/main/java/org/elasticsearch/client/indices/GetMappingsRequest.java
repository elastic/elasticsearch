/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.common.Strings;

public class GetMappingsRequest extends TimedRequest {

    private boolean local = false;
    private boolean includeDefaults = false;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions;

    /**
     * Indicates whether the receiving node should operate based on local index information or
     * forward requests, where needed, to other nodes. If running locally, request will not
     * raise errors if local index information is missing.
     */
    public GetMappingsRequest local(boolean local) {
        this.local = local;
        return this;
    }

    public boolean local() {
        return local;
    }

    public GetMappingsRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public GetMappingsRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public String[] indices() {
        return indices;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public boolean includeDefaults() {
        return includeDefaults;
    }

    /** Indicates whether default mapping settings should be returned */
    public GetMappingsRequest includeDefaults(boolean includeDefaults) {
        this.includeDefaults = includeDefaults;
        return this;
    }
}
