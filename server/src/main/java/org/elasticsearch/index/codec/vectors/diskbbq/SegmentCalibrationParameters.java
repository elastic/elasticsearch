/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Calibration-derived parameters for one vector field in one segment.
 * Each quantization method that supports auto-calibration must implement it to avoid missing data when reading from segments.
 */
public sealed interface SegmentCalibrationParameters permits SegmentCalibrationParameters.Osq {

    byte TYPE_OSQ = 0;

    void toXContent(XContentBuilder builder) throws IOException;

    void writeTo(StreamOutput out) throws IOException;

    boolean calibrated();

    String type();

    /**
     * Provides OSQ-specific auto-calibration parameters.
     */
    record Osq(QuantEncoding encoding, boolean precondition, float oversample) implements SegmentCalibrationParameters {

        @Override
        public String type() {
            return "osq";
        }

        @Override
        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field("bits", encoding.bits());
            builder.field("query_bits", encoding.queryBits());
            builder.field("precondition", precondition);
            builder.field("oversample", oversample);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(TYPE_OSQ);
            out.writeEnum(encoding);
            out.writeBoolean(precondition);
            out.writeFloat(oversample);
        }

        @Override
        public boolean calibrated() {
            return Float.isNaN(oversample) == false;
        }
    }

    static SegmentCalibrationParameters readFrom(StreamInput in) throws IOException {
        byte type = in.readByte();
        return switch (type) {
            case TYPE_OSQ -> new Osq(in.readEnum(QuantEncoding.class), in.readBoolean(), in.readFloat());
            default -> throw new IOException("Unknown calibration parameters type: " + type);
        };
    }
}
