/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.protocol.xpack.common.ProtocolUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class PostStartBasicResponse extends AcknowledgedResponse implements StatusToXContentObject {

    private static final ParseField BASIC_WAS_STARTED_FIELD = new ParseField("basic_was_started");
    private static final ParseField ERROR_MESSAGE_FIELD = new ParseField("error_message");
    private static final ParseField MESSAGE_FIELD = new ParseField("message");

    private final Map<String, String[]> acknowledgeMessages;
    private final String acknowledgeMessage;

    public enum Status {
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

        public boolean isBasicStarted() {
            return isBasicStarted;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        RestStatus getRestStatus() {
            return restStatus;
        }
    }

    private final Status status;

    public PostStartBasicResponse(StreamInput in) throws IOException {
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

    PostStartBasicResponse(Status status) {
        this(status, Collections.emptyMap(), null);
    }

    public PostStartBasicResponse(Status status, Map<String, String[]> acknowledgeMessages, String acknowledgeMessage) {
        super(status != Status.NEED_ACKNOWLEDGEMENT);
        this.status = status;
        this.acknowledgeMessages = acknowledgeMessages;
        this.acknowledgeMessage = acknowledgeMessage;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(status);
        out.writeOptionalString(acknowledgeMessage);
        out.writeMap(acknowledgeMessages, StreamOutput::writeString, StreamOutput::writeStringArray);
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        if (status.isBasicStarted()) {
            builder.field(BASIC_WAS_STARTED_FIELD.getPreferredName(), true);
        } else {
            builder.field(BASIC_WAS_STARTED_FIELD.getPreferredName(), false);
            builder.field(ERROR_MESSAGE_FIELD.getPreferredName(), status.getErrorMessage());
        }
        if (acknowledgeMessages.isEmpty() == false) {
            builder.startObject("acknowledge");
            builder.field(MESSAGE_FIELD.getPreferredName(), acknowledgeMessage);
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

    @Override
    public RestStatus status() {
        return status.restStatus;
    }

    public String getAcknowledgeMessage() {
        return acknowledgeMessage;
    }

    public Map<String, String[]> getAcknowledgeMessages() {
        return acknowledgeMessages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        PostStartBasicResponse that = (PostStartBasicResponse) o;

        return status == that.status
            && ProtocolUtils.equals(acknowledgeMessages, that.acknowledgeMessages)
            && Objects.equals(acknowledgeMessage, that.acknowledgeMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), status, ProtocolUtils.hashCode(acknowledgeMessages), acknowledgeMessage);
    }
}
