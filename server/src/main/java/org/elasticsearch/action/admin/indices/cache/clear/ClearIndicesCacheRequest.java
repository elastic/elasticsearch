/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.cache.clear;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ClearIndicesCacheRequest extends BroadcastRequest<ClearIndicesCacheRequest> {

    private boolean queryCache = false;
    private boolean fieldDataCache = false;
    private boolean requestCache = false;
    private String[] fields = Strings.EMPTY_ARRAY;

    public ClearIndicesCacheRequest(StreamInput in) throws IOException {
        super(in);
        queryCache = in.readBoolean();
        fieldDataCache = in.readBoolean();
        fields = in.readStringArray();
        requestCache = in.readBoolean();
    }

    public ClearIndicesCacheRequest(String... indices) {
        super(indices);
    }

    public boolean queryCache() {
        return queryCache;
    }

    public ClearIndicesCacheRequest queryCache(boolean queryCache) {
        this.queryCache = queryCache;
        return this;
    }

    public boolean requestCache() {
        return this.requestCache;
    }

    public ClearIndicesCacheRequest requestCache(boolean requestCache) {
        this.requestCache = requestCache;
        return this;
    }

    public boolean fieldDataCache() {
        return this.fieldDataCache;
    }

    public ClearIndicesCacheRequest fieldDataCache(boolean fieldDataCache) {
        this.fieldDataCache = fieldDataCache;
        return this;
    }

    public ClearIndicesCacheRequest fields(String... fields) {
        this.fields = fields == null ? Strings.EMPTY_ARRAY : fields;
        return this;
    }

    public String[] fields() {
        return this.fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(queryCache);
        out.writeBoolean(fieldDataCache);
        out.writeStringArrayNullable(fields);
        out.writeBoolean(requestCache);
    }
}
