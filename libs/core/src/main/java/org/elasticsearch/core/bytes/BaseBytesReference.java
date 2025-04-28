/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core.bytes;

public interface BaseBytesReference {
    /**
     * Returns the byte at the specified index. Need to be between 0 and length.
     */
    byte get(int index);

    /**
     * The length.
     */
    int length();

    /**
     * @return {@code true} if this instance is backed by a byte array
     */
    default boolean hasArray() {
        return false;
    }

    /**
     * @return backing byte array for this instance
     */
    default byte[] array() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return offset of the first byte of this instance in the backing byte array
     */
    default int arrayOffset() {
        throw new UnsupportedOperationException();
    }

}
