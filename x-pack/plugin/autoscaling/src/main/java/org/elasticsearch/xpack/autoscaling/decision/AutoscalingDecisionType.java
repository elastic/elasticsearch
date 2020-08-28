/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 * Represents the type of an autoscaling decision: to indicating if a scale down, no scaling event, or a scale up is needed.
 */
public enum AutoscalingDecisionType implements Writeable, ToXContentFragment {

    /**
     * Indicates that a scale down event is needed.
     */
    SCALE_DOWN((byte) 0),

    /**
     * Indicates that no scaling event is needed.
     */
    NO_SCALE((byte) 1),

    /**
     * Indicates that a scale up event is needed.
     */
    SCALE_UP((byte) 2);

    private final byte id;

    byte id() {
        return id;
    }

    AutoscalingDecisionType(final byte id) {
        this.id = id;
    }

    public static AutoscalingDecisionType readFrom(final StreamInput in) throws IOException {
        final byte id = in.readByte();
        switch (id) {
            case 0:
                return SCALE_DOWN;
            case 1:
                return NO_SCALE;
            case 2:
                return SCALE_UP;
            default:
                throw new IllegalArgumentException("unexpected value [" + id + "] for autoscaling decision type");
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeByte(id);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.value(name().toLowerCase(Locale.ROOT));
        return builder;
    }

}
