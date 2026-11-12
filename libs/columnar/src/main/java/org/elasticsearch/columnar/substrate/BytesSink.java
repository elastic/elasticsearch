/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import java.io.IOException;

/**
 * Takes bytes a piece at a time, so a value read out of several chunks can be handed on without first being put back
 * together. {@link ChunkedBytesWriter#append} is one.
 */
@FunctionalInterface
public interface BytesSink {

    /** Takes {@code length} bytes of {@code bytes} at {@code offset}; they are valid only for the duration of the call. */
    void append(byte[] bytes, int offset, int length) throws IOException;
}
