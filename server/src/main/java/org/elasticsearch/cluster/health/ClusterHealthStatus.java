/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
