/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public interface BytesTransportMessage {

    ReleasableBytesReference bytes();

    /**
     * Writes the data in a "thin" manner, without the actual bytes, assumes
     * the actual bytes will be appended right after this content.
     */
    void writeThin(StreamOutput out) throws IOException;
}
