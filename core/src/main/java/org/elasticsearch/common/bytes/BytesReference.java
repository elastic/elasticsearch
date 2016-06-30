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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A reference to bytes.
 */
public interface BytesReference extends Accountable {

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
    default StreamInput streamInput() {
        BytesRef ref = toBytesRef();
        return StreamInput.wrap(ref.bytes, ref.offset, ref.length);
    }

    /**
     * Writes the bytes directly to the output stream.
     */
    void writeTo(OutputStream os) throws IOException;

    /**
     * Converts to a string based on utf8.
     */
    default String toUtf8() {
        return toBytesRef().utf8ToString();
    }

    /**
     * Converts to Lucene BytesRef.
     */
    BytesRef toBytesRef();

    /**
     * Returns a BytesRefIterator for this BytesReference. This method allows
     * access to the internal pages of this reference without copying them. Use with care!
     * @see BytesRefIterator
     */
    default BytesRefIterator iterator() {
        return new BytesRefIterator() {
            BytesRef ref = length() == 0 ? null : toBytesRef();
            @Override
            public BytesRef next() throws IOException {
                BytesRef r = ref;
                ref = null; // only return it once...
                return r;
            }
        };
    }

    /**
     * Expert: compares two BytesReference instances against each other,
     * returning true if the bytes are equal.
     */
    static boolean bytesEqual(BytesReference a, BytesReference b) {
        if (a == b) {
            return true;
        }
        if (a.length() != b.length()) {
            return false;
        }
        for (int i = 0, end = a.length(); i < end; ++i) {
            if (a.get(i) != b.get(i)) {
                return false;
            }
        }
        return true;
    }


    static int hashCode(BytesReference a) {
        final BytesRefIterator iterator = a.iterator();
        BytesRef ref;
        int result = 1;
        try {
            while ((ref = iterator.next()) != null) {
                for (int i = 0; i < ref.length; i++) {
                    result = 31 * result + ref.bytes[ref.offset + i];
                }
            }
        } catch (IOException ex) {
            throw new AssertionError("wont happen", ex);
        }
        return result;
    }

    /**
     * Returns a compact array from the given BytesReference. The returned array won't be copied unless necessary. If you need
     * to modify the returned array use <tt>BytesRef.deepCopyOf(reference.toBytesRef()</tt> instead
     */
    static byte[] toBytes(BytesReference reference) {
        final BytesRef bytesRef = reference.toBytesRef();
        if (bytesRef.offset == 0 && bytesRef.length == bytesRef.bytes.length) {
            return bytesRef.bytes;
        }
        return BytesRef.deepCopyOf(bytesRef).bytes;
    }

}
