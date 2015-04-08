/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.ack;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A ack watch request to ack a watch by name (id)
 */
public class AckWatchRequest extends MasterNodeOperationRequest<AckWatchRequest> {

    private String id;

    public AckWatchRequest() {
    }

    public AckWatchRequest(String id) {
        this.id = id;
    }

    /**
     * @return The name of the watch to be acked
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the name of the watch to be acked
     */
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (id == null){
            validationException = ValidateActions.addValidationError("watch id is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    @Override
    public String toString() {
        return "ack [" + id + "]";
    }
}
