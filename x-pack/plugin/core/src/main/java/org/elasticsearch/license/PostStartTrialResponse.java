/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class PostStartTrialResponse extends ActionResponse {

    // Nodes Prior to 6.3 did not have NEED_ACKNOWLEDGEMENT as part of status
    enum Pre63Status {
        UPGRADED_TO_TRIAL,
        TRIAL_ALREADY_ACTIVATED;
    }
    enum Status {
        UPGRADED_TO_TRIAL(true, null, RestStatus.OK),
        TRIAL_ALREADY_ACTIVATED(false, "Operation failed: Trial was already activated.", RestStatus.FORBIDDEN),
        NEED_ACKNOWLEDGEMENT(false,"Operation failed: Needs acknowledgement.", RestStatus.OK);

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

    PostStartTrialResponse() {
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
    public void readFrom(StreamInput in) throws IOException {
        status = in.readEnum(Status.class);
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            acknowledgeMessage = in.readOptionalString();
            int size = in.readVInt();
            Map<String, String[]> acknowledgeMessages = new HashMap<>(size);
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
        } else {
            this.acknowledgeMessages = Collections.emptyMap();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Version version = Version.V_6_3_0;
        if (out.getVersion().onOrAfter(version)) {
            out.writeEnum(status);
            out.writeOptionalString(acknowledgeMessage);
            out.writeVInt(acknowledgeMessages.size());
            for (Map.Entry<String, String[]> entry : acknowledgeMessages.entrySet()) {
                out.writeString(entry.getKey());
                out.writeVInt(entry.getValue().length);
                for (String message : entry.getValue()) {
                    out.writeString(message);
                }
            }
        } else {
            if (status == Status.UPGRADED_TO_TRIAL) {
                out.writeEnum(Pre63Status.UPGRADED_TO_TRIAL);
            } else if (status == Status.TRIAL_ALREADY_ACTIVATED) {
                out.writeEnum(Pre63Status.TRIAL_ALREADY_ACTIVATED);
            } else {
                throw new IllegalArgumentException("Starting trial on node with version [" + Version.CURRENT + "] requires " +
                        "acknowledgement parameter.");
            }
        }
    }

    Map<String, String[]> getAcknowledgementMessages() {
        return acknowledgeMessages;
    }

    String getAcknowledgementMessage() {
        return acknowledgeMessage;
    }
}
