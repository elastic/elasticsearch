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
public abstract class BytesReference implements Accountable {

    /**
     * Returns the byte at the specified index. Need to be between 0 and length.
     */
    public abstract byte get(int index);

    /**
     * The length.
     */
    public abstract int length();

    /**
     * Slice the bytes from the <tt>from</tt> index up to <tt>length</tt>.
     */
    public abstract BytesReference slice(int from, int length);

    /**
     * A stream input of the bytes.
     */
    public StreamInput streamInput() {
        BytesRef ref = toBytesRef();
        return StreamInput.wrap(ref.bytes, ref.offset, ref.length);
    }

    /**
     * Writes the bytes directly to the output stream.
     */
    public abstract void writeTo(OutputStream os) throws IOException;

    /**
     * Converts to a string based on utf8.
     */
    public String toUtf8() {
        return toBytesRef().utf8ToString();
    }

    /**
     * Converts to Lucene BytesRef.
     */
    public abstract BytesRef toBytesRef();

    /**
     * Returns a BytesRefIterator for this BytesReference. This method allows
     * access to the internal pages of this reference without copying them. Use with care!
     * @see BytesRefIterator
     */
    public BytesRefIterator iterator() {
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

    @Override
    public boolean equals(Object b) {
        if (this == b) {
            return true;
        }
        if (b instanceof BytesReference) {
            BytesReference other = (BytesReference) b;
            if (this.length() != other.length()) {
                return false;
            }
            for (int i = 0; i < length(); ++i) {
                if (other.get(i) != other.get(i)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        final BytesRefIterator iterator = iterator();
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
    public static byte[] toBytes(BytesReference reference) {
        final BytesRef bytesRef = reference.toBytesRef();
        if (bytesRef.offset == 0 && bytesRef.length == bytesRef.bytes.length) {
            return bytesRef.bytes;
        }
        return BytesRef.deepCopyOf(bytesRef).bytes;
    }

}
