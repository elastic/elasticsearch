/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

public class BroadcastRequest<Request extends BroadcastRequest<Request>> extends ActionRequest implements IndicesRequest.Replaceable {

    protected String[] indices;
    private IndicesOptions indicesOptions;

    @Nullable // if timeout is infinite
    private TimeValue timeout;

    public BroadcastRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        timeout = in.readOptionalTimeValue();
    }

    protected BroadcastRequest(String... indices) {
        this(indices, IndicesOptions.strictExpandOpenAndForbidClosed());
    }

    protected BroadcastRequest(String[] indices, IndicesOptions indicesOptions) {
        this(indices, indicesOptions, null);
    }

    protected BroadcastRequest(String[] indices, IndicesOptions indicesOptions, @Nullable TimeValue timeout) {
        this.indices = indices;
        this.indicesOptions = indicesOptions;
        this.timeout = timeout;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final Request indices(String... indices) {
        this.indices = indices;
        return (Request) this;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @SuppressWarnings("unchecked")
    public final Request indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return (Request) this;
    }

    @Nullable // if timeout is infinite
    public TimeValue timeout() {
        return timeout;
    }

    @SuppressWarnings("unchecked")
    public final Request timeout(@Nullable TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalTimeValue(timeout);
    }
}
