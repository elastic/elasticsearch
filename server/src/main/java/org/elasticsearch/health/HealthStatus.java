/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.stream.Stream;

public enum HealthStatus implements Writeable {

    GREEN((byte) 0),
    YELLOW((byte) 1),
    RED((byte) 2);

    private final byte value;

    HealthStatus(byte value) {
        this.value = value;
    }

    public static HealthStatus aggregate(Stream<HealthStatus> statusStream) {
        return statusStream.max(HealthStatus::compareTo).orElse(GREEN);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(value);
    }
}
