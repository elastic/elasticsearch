/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public enum MetricQuality {
    EXACT((byte) 0, "exact"),
    MINIMUM((byte) 1, "minimum"),
    MISSING((byte) 2, "missing");

    private final byte id;
    private final String label;

    MetricQuality(byte value, String label) {
        this.id = value;
        this.label = label;
    }

    public byte getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public static MetricQuality fromId(byte id) {
        return switch (id) {
            case 0 -> EXACT;
            case 1 -> MINIMUM;
            case 2 -> MISSING;
            default -> throw new IllegalStateException("No metric quality for [" + id + "]");
        };
    }

    public static MetricQuality readFrom(StreamInput in) throws IOException {
        return MetricQuality.fromId(in.readByte());
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(getId());
    }
}
