/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.license;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * Status of an X-Pack license.
 */
public enum LicenseStatus implements Writeable {

    ACTIVE("active"),
    INVALID("invalid"),
    EXPIRED("expired");

    private final String label;

    LicenseStatus(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(label);
    }

    public static LicenseStatus readFrom(StreamInput in) throws IOException {
        return fromString(in.readString());
    }

    public static LicenseStatus fromString(String value) {
        switch (value) {
            case "active":
                return ACTIVE;
            case "invalid":
                return INVALID;
            case "expired":
                return EXPIRED;
            default:
                throw new IllegalArgumentException("unknown license status [" + value + "]");
        }
    }
}
