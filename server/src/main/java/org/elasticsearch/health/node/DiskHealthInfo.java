/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.health.HealthStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The health status of the disk space of this node along with the cause.
 */
public record DiskHealthInfo(HealthStatus healthStatus, Cause cause) implements Writeable {
    DiskHealthInfo(HealthStatus healthStatus) {
        this(healthStatus, null);
    }

    public DiskHealthInfo(StreamInput in) throws IOException {
        this(HealthStatus.from(in), DiskHealthInfo.Cause.from(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        healthStatus.writeTo(out);
        cause.writeTo(out);
    }

    public enum Cause implements Writeable {
        NODE_OVER_HIGH_THRESHOLD((byte) 0),
        NODE_OVER_THE_FLOOD_STAGE_THRESHOLD((byte) 1),
        FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD((byte) 2),
        NODE_HAS_NO_DISK_STATS((byte) 3);

        private static final Map<Byte, Cause> values = Arrays.stream(Cause.values())
            .collect(Collectors.toMap(Cause::value, Function.identity()));

        private final byte value;

        Cause(byte value) {
            this.value = value;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(value);
        }

        public byte value() {
            return value;
        }

        public static DiskHealthInfo.Cause from(StreamInput in) throws IOException {
            byte b = in.readByte();
            DiskHealthInfo.Cause cause = values.get(b);
            if (cause != null) {
                return cause;
            }
            throw new IllegalArgumentException("Cannot deserialize disk health indo cause with id " + b);
        }
    }
}
