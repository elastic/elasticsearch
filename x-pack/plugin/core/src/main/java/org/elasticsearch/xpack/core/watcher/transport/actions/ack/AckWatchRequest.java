/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.ack;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.watcher.support.WatcherUtils;

import java.io.IOException;
import java.util.Locale;

/**
 * A ack watch request to ack a watch by name (id)
 */
public class AckWatchRequest extends ActionRequest {

    private String watchId;
    private String[] actionIds = Strings.EMPTY_ARRAY;

    public AckWatchRequest() {
        this(null, (String[]) null);
    }

    public AckWatchRequest(String watchId, String... actionIds) {
        this.watchId = watchId;
        this.actionIds = actionIds;
    }

    public AckWatchRequest(StreamInput in) throws IOException {
        super(in);
        watchId = in.readString();
        actionIds = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(watchId);
        out.writeStringArray(actionIds);
    }

    /**
     * @return The id of the watch to be acked
     */
    public String getWatchId() {
        return watchId;
    }

    /**
     * @param actionIds The ids of the actions to be acked
     */
    public void setActionIds(String... actionIds) {
        this.actionIds = actionIds;
    }

    /**
     * @return The ids of the actions to be acked
     */
    public String[] getActionIds() {
        return actionIds;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (watchId == null) {
            validationException = ValidateActions.addValidationError("watch id is missing", validationException);
        } else if (WatcherUtils.isValidId(watchId) == false) {
            validationException = ValidateActions.addValidationError("watch id contains whitespace", validationException);
        }
        if (actionIds != null) {
            for (String actionId : actionIds) {
                if (actionId == null) {
                    validationException = ValidateActions.addValidationError(
                        String.format(Locale.ROOT, "action id may not be null"),
                        validationException
                    );
                } else if (WatcherUtils.isValidId(actionId) == false) {
                    validationException = ValidateActions.addValidationError(
                        String.format(Locale.ROOT, "action id [%s] contains whitespace", actionId),
                        validationException
                    );
                }
            }
        }
        return validationException;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ack [").append(watchId).append("]");
        if (actionIds.length > 0) {
            sb.append("[");
            for (int i = 0; i < actionIds.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(actionIds[i]);
            }
            sb.append("]");
        }
        return sb.toString();
    }
}
