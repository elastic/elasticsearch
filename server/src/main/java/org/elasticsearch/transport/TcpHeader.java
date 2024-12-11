/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class TcpHeader {

    public static final int MARKER_BYTES_SIZE = 2;

    public static final int MESSAGE_LENGTH_SIZE = 4;

    public static final int REQUEST_ID_SIZE = 8;

    public static final int STATUS_SIZE = 1;

    public static final int VERSION_ID_SIZE = 4;

    public static final int VARIABLE_HEADER_SIZE = 4;

    public static final int BYTES_REQUIRED_FOR_MESSAGE_SIZE = MARKER_BYTES_SIZE + MESSAGE_LENGTH_SIZE;

    public static final int VERSION_POSITION = MARKER_BYTES_SIZE + MESSAGE_LENGTH_SIZE + REQUEST_ID_SIZE + STATUS_SIZE;

    public static final int VARIABLE_HEADER_SIZE_POSITION = VERSION_POSITION + VERSION_ID_SIZE;

    public static final int BYTES_REQUIRED_FOR_VERSION = VERSION_POSITION + VERSION_ID_SIZE;

    public static final int HEADER_SIZE = BYTES_REQUIRED_FOR_VERSION + VARIABLE_HEADER_SIZE;

    private static final byte[] PREFIX = { (byte) 'E', (byte) 'S' };

    public static void writeHeader(
        StreamOutput output,
        long requestId,
        byte status,
        TransportVersion version,
        int contentSize,
        int variableHeaderSize
    ) throws IOException {
        output.writeBytes(PREFIX);
        // write the size, the size indicates the remaining message size, not including the size int
        output.writeInt(contentSize + REQUEST_ID_SIZE + STATUS_SIZE + VERSION_ID_SIZE + VARIABLE_HEADER_SIZE);
        output.writeLong(requestId);
        output.writeByte(status);
        output.writeInt(version.id());
        output.writeInt(variableHeaderSize);
    }
}
