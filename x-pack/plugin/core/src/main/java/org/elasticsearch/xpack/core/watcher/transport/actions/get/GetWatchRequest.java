/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.get;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.watcher.support.WatcherUtils;

import java.io.IOException;

/**
 * The request to get the watch by name (id)
 */
public class GetWatchRequest extends ActionRequest {

    private String id;

    public GetWatchRequest() {
    }

    /**
     * @param id name (id) of the watch to retrieve
     */
    public GetWatchRequest(String id) {
        this.id = id;
    }

    public GetWatchRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    GetWatchRequest setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (id == null) {
            validationException = ValidateActions.addValidationError("watch id is missing", validationException);
        } else if (WatcherUtils.isValidId(id) == false) {
            validationException = ValidateActions.addValidationError("watch id contains whitespace", validationException);
        }

        return validationException;
    }


    /**
     * @return The name of the watch to retrieve
     */
    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "get [" + id +"]";
    }
}
