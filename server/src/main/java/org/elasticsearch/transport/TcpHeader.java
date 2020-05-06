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

    public static final Version VERSION_WITH_HEADER_SIZE = Version.V_7_6_0;

    public static final int MARKER_BYTES_SIZE = 2;

    public static final int MESSAGE_LENGTH_SIZE = 4;

    public static final int REQUEST_ID_SIZE = 8;

    public static final int STATUS_SIZE = 1;

    public static final int VERSION_ID_SIZE = 4;

    public static final int VARIABLE_HEADER_SIZE = 4;

    public static final int BYTES_REQUIRED_FOR_MESSAGE_SIZE = MARKER_BYTES_SIZE + MESSAGE_LENGTH_SIZE;

    public static final int VERSION_POSITION = MARKER_BYTES_SIZE + MESSAGE_LENGTH_SIZE + REQUEST_ID_SIZE + STATUS_SIZE;

    public static final int VARIABLE_HEADER_SIZE_POSITION = VERSION_POSITION + VERSION_ID_SIZE;

    private static final int PRE_76_HEADER_SIZE = VERSION_POSITION + VERSION_ID_SIZE;

    public static final int BYTES_REQUIRED_FOR_VERSION = PRE_76_HEADER_SIZE;

    private static final int HEADER_SIZE = PRE_76_HEADER_SIZE + VARIABLE_HEADER_SIZE;

    public static int headerSize(Version version) {
        if (version.onOrAfter(VERSION_WITH_HEADER_SIZE)) {
            return HEADER_SIZE;
        } else {
            return PRE_76_HEADER_SIZE;
        }
    }

    public static void writeHeader(StreamOutput output, long requestId, byte status, Version version, int contentSize,
                                   int variableHeaderSize) throws IOException {
        output.writeByte((byte)'E');
        output.writeByte((byte)'S');
        // write the size, the size indicates the remaining message size, not including the size int
        if (version.onOrAfter(VERSION_WITH_HEADER_SIZE)) {
            output.writeInt(contentSize + REQUEST_ID_SIZE + STATUS_SIZE + VERSION_ID_SIZE + VARIABLE_HEADER_SIZE);
        } else {
            output.writeInt(contentSize + REQUEST_ID_SIZE + STATUS_SIZE + VERSION_ID_SIZE);
        }
        output.writeLong(requestId);
        output.writeByte(status);
        output.writeInt(version.id);
        if (version.onOrAfter(VERSION_WITH_HEADER_SIZE)) {
            assert variableHeaderSize != -1 : "Variable header size not set";
            output.writeInt(variableHeaderSize);
        }
    }
}
