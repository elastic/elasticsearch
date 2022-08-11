/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum HealthStatus implements Writeable {
    GREEN((byte) 0),
    UNKNOWN((byte) 1),
    YELLOW((byte) 2),
    RED((byte) 3);

    private final byte value;
    private static final Map<Byte, HealthStatus> values = Arrays.stream(HealthStatus.values())
        .collect(Collectors.toMap(HealthStatus::value, Function.identity()));

    HealthStatus(byte value) {
        this.value = value;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(value);
    }

    public static HealthStatus from(StreamInput in) throws IOException {
        byte b = in.readByte();
        HealthStatus healthStatus = values.get(b);
        if (healthStatus != null) {
            return healthStatus;
        }
        throw new IllegalArgumentException("Cannot deserialize health with id " + b);
    }

    public byte value() {
        return value;
    }

    /**
     * @return true if the HealthStatus is YELLOW or RED.
     */
    public boolean indicatesHealthProblem() {
        return value > UNKNOWN.value();
    }

    public static HealthStatus merge(Stream<HealthStatus> statuses) {
        return statuses.max(Comparator.comparing(HealthStatus::value))
            .orElseThrow(() -> new IllegalArgumentException("Cannot merge empty health status stream."));
    }

    public String xContentValue() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static HealthStatus fromCoordinationDiagnosticsStatus(
        CoordinationDiagnosticsService.CoordinationDiagnosticsStatus coordinationDiagnosticsStatus
    ) {
        return switch (coordinationDiagnosticsStatus) {
            case GREEN -> HealthStatus.GREEN;
            case YELLOW -> HealthStatus.YELLOW;
            case RED -> HealthStatus.RED;
            case UNKNOWN -> HealthStatus.UNKNOWN;
        };
    }
}
