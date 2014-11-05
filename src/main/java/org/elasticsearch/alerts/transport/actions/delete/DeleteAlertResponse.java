/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.delete;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class DeleteAlertResponse extends ActionResponse {

    private DeleteResponse deleteResponse;

    public DeleteAlertResponse() {
    }

    public DeleteAlertResponse(@Nullable DeleteResponse deleteResponse) {
        this.deleteResponse = deleteResponse;
    }

    public DeleteResponse deleteResponse() {
        return deleteResponse;
    }

    public void deleteResponse(DeleteResponse deleteResponse){
        this.deleteResponse = deleteResponse;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        deleteResponse = new DeleteResponse();
        if (in.readBoolean()) {
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
