/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.bytes;

import org.elasticsearch.common.io.stream.StreamInput;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;

/**
 * A reference to bytes.
 */
public interface BytesReference {

    public static class Helper {

        public static boolean bytesEqual(BytesReference a, BytesReference b) {
            if (a == b) {
                return true;
            }
            if (a.length() != b.length()) {
                return false;
            }
            if (!a.hasArray()) {
                a = a.toBytesArray();
            }
            if (!b.hasArray()) {
                b = b.toBytesArray();
            }
            int bUpTo = b.arrayOffset();
            final byte[] aArray = a.array();
            final byte[] bArray = b.array();
            final int end = a.arrayOffset() + a.length();
            for (int aUpTo = a.arrayOffset(); aUpTo < end; aUpTo++, bUpTo++) {
                if (aArray[aUpTo] != bArray[bUpTo]) {
                    return false;
                }
            }
            return true;
        }

        public static int bytesHashCode(BytesReference a) {
            if (!a.hasArray()) {
                a = a.toBytesArray();
            }
            int result = 0;
            final int end = a.arrayOffset() + a.length();
            for (int i = a.arrayOffset(); i < end; i++) {
                result = 31 * result + a.array()[i];
            }
            return result;
        }
    }

    /**
     * Returns the byte at the specified index. Need to be between 0 and length.
     */
    byte get(int index);

    /**
     * The length.
     */
    int length();

    /**
     * Slice the bytes from the <tt>from</tt> index up to <tt>length</tt>.
     */
    BytesReference slice(int from, int length);

    /**
     * A stream input of the bytes.
     */
    StreamInput streamInput();

    /**
     * Writes the bytes directly to the output stream.
     */
    void writeTo(OutputStream os) throws IOException;

    /**
     * Returns the bytes as a single byte array.
     */
    byte[] toBytes();

    /**
     * Returns the bytes as a byte array, possibly sharing the underlying byte buffer.
     */
    BytesArray toBytesArray();

    /**
     * Returns the bytes copied over as a byte array.
     */
    BytesArray copyBytesArray();

    /**
     * Returns the bytes as a channel buffer.
     */
    ChannelBuffer toChannelBuffer();

    /**
     * Is there an underlying byte array for this bytes reference.
     */
    boolean hasArray();

    /**
     * The underlying byte array (if exists).
     */
    byte[] array();

    /**
     * The offset into the underlying byte array.
     */
    int arrayOffset();

    /**
     * Converts to a string based on utf8.
     */
    String toUtf8();


    // LUCENE 4 UPGRADE: Used by facets to order. Perhaps make this call implement Comparable.
    public final static Comparator<BytesReference> utf8SortedAsUnicodeSortOrder = new UTF8SortedAsUnicodeComparator();

    public static class UTF8SortedAsUnicodeComparator implements Comparator<BytesReference> {

        // Only singleton
        private UTF8SortedAsUnicodeComparator() {
        }

        public int compare(BytesReference a, BytesReference b) {
            if (a.hasArray() && b.hasArray()) {
                final byte[] aBytes = a.array();
                int aUpto = a.arrayOffset();
                final byte[] bBytes = b.array();
                int bUpto = b.arrayOffset();

                final int aStop = aUpto + Math.min(a.length(), b.length());
                while (aUpto < aStop) {
                    int aByte = aBytes[aUpto++] & 0xff;
                    int bByte = bBytes[bUpto++] & 0xff;

                    int diff = aByte - bByte;
                    if (diff != 0) {
                        return diff;
                    }
                }

                // One is a prefix of the other, or, they are equal:
                return a.length() - b.length();
            } else {
                final byte[] aBytes = a.toBytes();
                int aUpto = 0;
                final byte[] bBytes = b.toBytes();
                int bUpto = 0;

                final int aStop = aUpto + Math.min(a.length(), b.length());
                while (aUpto < aStop) {
                    int aByte = aBytes[aUpto++] & 0xff;
                    int bByte = bBytes[bUpto++] & 0xff;

                    int diff = aByte - bByte;
                    if (diff != 0) {
                        return diff;
                    }
                }

                // One is a prefix of the other, or, they are equal:
                return a.length() - b.length();
            }
        }
    }
}
