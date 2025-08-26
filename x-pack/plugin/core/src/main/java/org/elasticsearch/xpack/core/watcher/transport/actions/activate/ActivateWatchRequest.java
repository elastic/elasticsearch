/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.activate;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.watcher.support.WatcherUtils;

import java.io.IOException;

/**
 * A ack watch request to ack a watch by name (id)
 */
public class ActivateWatchRequest extends LegacyActionRequest {

    private String watchId;
    private boolean activate;

    public ActivateWatchRequest() {
        this(null, true);
    }

    public ActivateWatchRequest(String watchId, boolean activate) {
        this.watchId = watchId;
        this.activate = activate;
    }

    public ActivateWatchRequest(StreamInput in) throws IOException {
        super(in);
        watchId = in.readString();
        activate = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(watchId);
        out.writeBoolean(activate);
    }

    /**
     * @return The id of the watch to be acked
     */
    public String getWatchId() {
        return watchId;
    }

    /**
     * @return {@code true} if the request is for activating the watch, {@code false} if its
     * for deactivating it.
     */
    public boolean isActivate() {
        return activate;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (watchId == null) {
            validationException = ValidateActions.addValidationError("watch id is missing", validationException);
        } else if (WatcherUtils.isValidId(watchId) == false) {
            validationException = ValidateActions.addValidationError("watch id contains whitespace", validationException);
        }
        return validationException;
    }

    @Override
    public String toString() {
        return activate ? "activate [" + watchId + "]" : "deactivate [" + watchId + "]";
    }
}
