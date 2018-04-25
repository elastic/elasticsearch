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

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class TcpHeader {
    public static final int MARKER_BYTES_SIZE = 2 * 1;

    public static final int MESSAGE_LENGTH_SIZE = 4;

    public static final int REQUEST_ID_SIZE = 8;

    public static final int STATUS_SIZE = 1;

    public static final int VERSION_ID_SIZE = 4;

    public static final int HEADER_SIZE = MARKER_BYTES_SIZE + MESSAGE_LENGTH_SIZE + REQUEST_ID_SIZE + STATUS_SIZE + VERSION_ID_SIZE;

    public static void writeHeader(StreamOutput output, long requestId, byte status, Version version, int messageSize) throws IOException {
        output.writeByte((byte)'E');
        output.writeByte((byte)'S');
        // write the size, the size indicates the remaining message size, not including the size int
        output.writeInt(messageSize + REQUEST_ID_SIZE + STATUS_SIZE + VERSION_ID_SIZE);
        output.writeLong(requestId);
        output.writeByte(status);
        output.writeInt(version.id);
    }
}
