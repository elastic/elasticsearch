/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling;

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
