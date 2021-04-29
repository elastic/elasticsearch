/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.health;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public enum ClusterHealthStatus implements Writeable {
    GREEN((byte) 0),
    YELLOW((byte) 1),
    RED((byte) 2);

    private byte value;

    ClusterHealthStatus(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(value);
    }

    /**
     * Read from a stream.
     *
     * @throws IllegalArgumentException if the value is unrecognized
     */
    public static ClusterHealthStatus readFrom(StreamInput in) throws IOException {
        byte value = in.readByte();
        switch (value) {
            case 0:
                return GREEN;
            case 1:
                return YELLOW;
            case 2:
                return RED;
            default:
                throw new IllegalArgumentException("No cluster health status for value [" + value + "]");
        }
    }

    public static ClusterHealthStatus fromString(String status) {
        if (status.equalsIgnoreCase("green")) {
            return GREEN;
        } else if (status.equalsIgnoreCase("yellow")) {
            return YELLOW;
        } else if (status.equalsIgnoreCase("red")) {
            return RED;
        } else {
            throw new IllegalArgumentException("unknown cluster health status [" + status + "]");
        }
    }
}
