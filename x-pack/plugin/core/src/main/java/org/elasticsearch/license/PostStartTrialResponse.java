/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class PostStartTrialResponse extends ActionResponse {

    public enum Status {
        UPGRADED_TO_TRIAL(true, null, RestStatus.OK),
        TRIAL_ALREADY_ACTIVATED(false, "Operation failed: Trial was already activated.", RestStatus.FORBIDDEN),
        NEED_ACKNOWLEDGEMENT(false, "Operation failed: Needs acknowledgement.", RestStatus.OK);

        private final boolean isTrialStarted;

        private final String errorMessage;
        private final RestStatus restStatus;

        Status(boolean isTrialStarted, String errorMessage, RestStatus restStatus) {
            this.isTrialStarted = isTrialStarted;
            this.errorMessage = errorMessage;
            this.restStatus = restStatus;
        }

        boolean isTrialStarted() {
            return isTrialStarted;
        }

        String getErrorMessage() {
            return errorMessage;
        }

        RestStatus getRestStatus() {
            return restStatus;
        }

    }

    private Status status;
    private Map<String, String[]> acknowledgeMessages;
    private String acknowledgeMessage;

    PostStartTrialResponse(StreamInput in) throws IOException {
        super(in);
        status = in.readEnum(Status.class);
        acknowledgeMessage = in.readOptionalString();
        int size = in.readVInt();
        Map<String, String[]> acknowledgeMessages = Maps.newMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            String feature = in.readString();
            int nMessages = in.readVInt();
            String[] messages = new String[nMessages];
            for (int j = 0; j < nMessages; j++) {
                messages[j] = in.readString();
            }
            acknowledgeMessages.put(feature, messages);
        }
        this.acknowledgeMessages = acknowledgeMessages;
    }

    PostStartTrialResponse(Status status) {
        this(status, Collections.emptyMap(), null);
    }

    PostStartTrialResponse(Status status, Map<String, String[]> acknowledgeMessages, String acknowledgeMessage) {
        this.status = status;
        this.acknowledgeMessages = acknowledgeMessages;
        this.acknowledgeMessage = acknowledgeMessage;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
        out.writeOptionalString(acknowledgeMessage);
        out.writeMap(acknowledgeMessages, StreamOutput::writeString, StreamOutput::writeStringArray);
    }

    Map<String, String[]> getAcknowledgementMessages() {
        return acknowledgeMessages;
    }

    String getAcknowledgementMessage() {
        return acknowledgeMessage;
    }
}
