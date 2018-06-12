/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PutLicenseResponse extends AcknowledgedResponse {

    private LicensesStatus status;
    private Map<String, String[]> acknowledgeMessages;
    private String acknowledgeHeader;

    PutLicenseResponse() {
    }

    public PutLicenseResponse(boolean acknowledged, LicensesStatus status) {
        this(acknowledged, status, null, Collections.<String, String[]>emptyMap());
    }

    public PutLicenseResponse(boolean acknowledged, LicensesStatus status, String acknowledgeHeader,
                              Map<String, String[]> acknowledgeMessages) {
        super(acknowledged);
        this.status = status;
        this.acknowledgeHeader = acknowledgeHeader;
        this.acknowledgeMessages = acknowledgeMessages;
    }

    public LicensesStatus status() {
        return status;
    }

    public Map<String, String[]> acknowledgeMessages() {
        return acknowledgeMessages;
    }

    public String acknowledgeHeader() {
        return acknowledgeHeader;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        status = LicensesStatus.fromId(in.readVInt());
        acknowledgeHeader = in.readOptionalString();
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
        out.writeVInt(status.id());
        out.writeOptionalString(acknowledgeHeader);
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
        switch (status) {
            case VALID:
                builder.field("license_status", "valid");
                break;
            case INVALID:
                builder.field("license_status", "invalid");
                break;
            case EXPIRED:
                builder.field("license_status", "expired");
                break;
            default:
                throw new IllegalArgumentException("unknown status [" + status + "] found");
        }
        if (!acknowledgeMessages.isEmpty()) {
            builder.startObject("acknowledge");
            builder.field("message", acknowledgeHeader);
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
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
