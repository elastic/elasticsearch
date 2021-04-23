/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    private static final byte[] PREFIX = {(byte) 'E', (byte) 'S'};

    public static void writeHeader(StreamOutput output, long requestId, byte status, Version version, int contentSize,
                                   int variableHeaderSize) throws IOException {
        output.writeBytes(PREFIX);
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
