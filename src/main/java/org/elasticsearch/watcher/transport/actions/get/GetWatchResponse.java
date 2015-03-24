/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetWatchResponse extends ActionResponse {

    private GetResponse getResponse;

    public GetWatchResponse() {
    }

    public GetWatchResponse(GetResponse getResponse) {
        this.getResponse = getResponse;
    }

    /**
     * Sets the GetResponse containing the watch source
     */
    public void getResponse(GetResponse getResponse) {
        this.getResponse = getResponse;
    }

    public GetResponse getResponse() {
        return this.getResponse;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            getResponse = GetResponse.readGetResponse(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(getResponse != null);
        if (getResponse != null) {
            getResponse.writeTo(out);
        }
    }
}
