/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.license.PostStartBasicResponse.Status.NEED_ACKNOWLEDGEMENT;

class PostStartBasicResponse extends AcknowledgedResponse {

    private Map<String, String[]> acknowledgeMessages;
    private String acknowledgeMessage;

    enum Status {
        GENERATED_BASIC(true, null, RestStatus.OK),
        ALREADY_USING_BASIC(false, "Operation failed: Current license is basic.", RestStatus.FORBIDDEN),
        NEED_ACKNOWLEDGEMENT(false, "Operation failed: Needs acknowledgement.", RestStatus.OK);

        private final boolean isBasicStarted;
        private final String errorMessage;
        private final RestStatus restStatus;

        Status(boolean isBasicStarted, String errorMessage, RestStatus restStatus) {
            this.isBasicStarted = isBasicStarted;
            this.errorMessage = errorMessage;
            this.restStatus = restStatus;
        }

        boolean isBasicStarted() {
            return isBasicStarted;
        }

        String getErrorMessage() {
            return errorMessage;
        }

        RestStatus getRestStatus() {
            return restStatus;
        }
    }

    private Status status;

    PostStartBasicResponse() {
    }

    PostStartBasicResponse(Status status) {
        this(status, Collections.emptyMap(), null);
    }

    PostStartBasicResponse(Status status, Map<String, String[]> acknowledgeMessages, String acknowledgeMessage) {
        super(status != NEED_ACKNOWLEDGEMENT);
        this.status = status;
        this.acknowledgeMessages = acknowledgeMessages;
        this.acknowledgeMessage = acknowledgeMessage;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        status = in.readEnum(Status.class);
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        if (status.isBasicStarted()) {
            builder.field("basic_was_started", true);
        } else {
            builder.field("basic_was_started", false);
            builder.field("error_message", status.getErrorMessage());
        }
        if (acknowledgeMessages.isEmpty() == false) {
            builder.startObject("acknowledge");
            builder.field("message", acknowledgeMessage);
            for (Map.Entry<String, String[]> entry : acknowledgeMessages.entrySet()) {
                builder.startArray(entry.getKey());
                for (String message : entry.getValue()) {
                    builder.value(message);
                }
                builder.endArray();
            }
            builder.endObject();
        }
    }
}
