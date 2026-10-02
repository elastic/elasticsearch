/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class BytesTransportMessageTestUtils {

    /**
     * Serializes a {@link BytesTransportMessage} the way test infrastructure expects on the wire:
     * {@link BytesTransportMessage#writeThin(StreamOutput)} followed by the raw bytes payload.
     */
    public static void writeThinWithBytes(StreamOutput out, BytesTransportMessage message) throws IOException {
        message.writeThin(out);
        message.bytes().writeTo(out);
    }
}
