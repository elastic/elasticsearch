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
package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.GatheringByteChannel;

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

            return bytesEquals(a, b);
        }

        // pkg-private for testing
        static boolean bytesEquals(BytesReference a, BytesReference b) {
            assert a.length() == b.length();
            for (int i = 0, end = a.length(); i < end; ++i) {
                if (a.get(i) != b.get(i)) {
                    return false;
                }
            }

            return true;
        }

        public static int bytesHashCode(BytesReference a) {
            if (a.hasArray()) {
                return hashCode(a.array(), a.arrayOffset(), a.length());
            } else {
                return slowHashCode(a);
            }
        }

        // pkg-private for testing
        static int hashCode(byte[] array, int offset, int length) {
            int result = 1;
            for (int i = offset, end = offset + length; i < end; ++i) {
                result = 31 * result + array[i];
            }
            return result;
        }

        // pkg-private for testing
        static int slowHashCode(BytesReference a) {
            int result = 1;
            for (int i = 0, end = a.length(); i < end; ++i) {
                result = 31 * result + a.get(i);
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
     * Writes the bytes directly to the channel.
     */
    void writeTo(GatheringByteChannel channel) throws IOException;

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

    /**
     * Converts to Lucene BytesRef.
     */
    BytesRef toBytesRef();

    /**
     * Converts to a copied Lucene BytesRef.
     */
    BytesRef copyBytesRef();
}
