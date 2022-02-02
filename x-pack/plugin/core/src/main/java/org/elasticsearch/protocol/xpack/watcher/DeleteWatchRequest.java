/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;

import java.io.IOException;

/**
 * A delete watch request to delete an watch by name (id)
 */
public class DeleteWatchRequest extends ActionRequest {

    private String id;
    private long version = Versions.MATCH_ANY;

    public DeleteWatchRequest() {}

    public DeleteWatchRequest(String id) {
        this.id = id;
    }

    public DeleteWatchRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        version = in.readLong();
    }

    /**
     * @return The name of the watch to be deleted
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the name of the watch to be deleted
     */
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (id == null) {
            validationException = ValidateActions.addValidationError("watch id is missing", validationException);
        } else if (PutWatchRequest.isValidId(id) == false) {
            validationException = ValidateActions.addValidationError("watch id contains whitespace", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeLong(version);
    }

    @Override
    public String toString() {
        return "delete [" + id + "]";
    }
}
