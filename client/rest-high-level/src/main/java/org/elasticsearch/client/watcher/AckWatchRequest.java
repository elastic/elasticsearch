/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;

import java.util.Locale;

/**
 * A request to explicitly acknowledge a watch.
 */
public class AckWatchRequest implements Validatable {

    private final String watchId;
    private final String[] actionIds;

    public AckWatchRequest(String watchId, String... actionIds) {
        validateIds(watchId, actionIds);
        this.watchId = watchId;
        this.actionIds = actionIds;
    }

    private void validateIds(String watchId, String... actionIds) {
        ValidationException exception = new ValidationException();
        if (watchId == null) {
            exception.addValidationError("watch id is missing");
        } else if (PutWatchRequest.isValidId(watchId) == false) {
            exception.addValidationError("watch id contains whitespace");
        }

        if (actionIds != null) {
            for (String actionId : actionIds) {
                if (actionId == null) {
                    exception.addValidationError(String.format(Locale.ROOT, "action id may not be null"));
                } else if (PutWatchRequest.isValidId(actionId) == false) {
                    exception.addValidationError(
                        String.format(Locale.ROOT, "action id [%s] contains whitespace", actionId));
                }
            }
        }

        if (exception.validationErrors().isEmpty() == false) {
            throw exception;
        }
    }

    /**
     * @return The ID of the watch to be acked.
     */
    public String getWatchId() {
        return watchId;
    }

    /**
     * @return The IDs of the actions to be acked. If omitted,
     * all actions for the given watch will be acknowledged.
     */
    public String[] getActionIds() {
        return actionIds;
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
