/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.delete;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class DeleteWatchResponse extends ActionResponse {

    private DeleteResponse deleteResponse;

    public DeleteWatchResponse() {
    }

    public DeleteWatchResponse(@Nullable DeleteResponse deleteResponse) {
        this.deleteResponse = deleteResponse;
    }

    public DeleteResponse deleteResponse() {
        return deleteResponse;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            deleteResponse = new DeleteResponse();
            deleteResponse.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(deleteResponse != null);
        if (deleteResponse != null) {
            deleteResponse.writeTo(out);
        }
    }
}
