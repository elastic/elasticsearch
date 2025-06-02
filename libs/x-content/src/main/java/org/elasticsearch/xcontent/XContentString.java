/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import java.nio.ByteBuffer;

public interface XContentString {
    record EncodedBytes(byte[] bytes, int offset, int length) implements Comparable<EncodedBytes> {
        public EncodedBytes(byte[] bytes) {
            this(bytes, 0, bytes.length);
        }

        @Override
        public int compareTo(EncodedBytes o) {
            return ByteBuffer.wrap(bytes, offset, length).compareTo(ByteBuffer.wrap(o.bytes, o.offset, o.length));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            return this.compareTo((EncodedBytes) o) == 0;
        }

        @Override
        public int hashCode() {
            return ByteBuffer.wrap(bytes, offset, length).hashCode();
        }
    }

    /**
     * Returns a {@link String} view of the data.
     */
    String string();

    /**
     * Returns a UTF8-encoded {@link ByteBuffer} view of the data.
     */
    EncodedBytes bytes();

    /**
     * Returns the number of characters in the represented string.
     */
    int stringLength();
}
