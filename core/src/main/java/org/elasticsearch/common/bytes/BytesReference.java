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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.ToIntBiFunction;

/**
 * A reference to bytes.
 */
public abstract class BytesReference implements Accountable, Comparable<BytesReference> {

    private Integer hash = null; // we cache the hash of this reference since it can be quite costly to re-calculated it

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
    public StreamInput streamInput() throws IOException {
        return new MarkSupportingStreamInputWrapper(this);
    }

    /**
     * Writes the bytes directly to the output stream.
     */
    public void writeTo(OutputStream os) throws IOException {
        final BytesRefIterator iterator = iterator();
        BytesRef ref;
        while ((ref = iterator.next()) != null) {
            os.write(ref.bytes, ref.offset, ref.length);
        }
    }

    /**
     * Interprets the referenced bytes as UTF8 bytes, returning the resulting string
     */
    public String utf8ToString() {
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
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof BytesReference) {
            final BytesReference otherRef = (BytesReference) other;
            if (length() != otherRef.length()) {
                return false;
            }
            return compareIterators(this, otherRef, (a, b) ->
                a.bytesEquals(b) ? 0 : 1 // this is a call to BytesRef#bytesEquals - this method is the hot one in the comparison
            ) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (hash == null) {
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
            return hash = result;
        } else {
            return hash.intValue();
        }
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

    @Override
    public int compareTo(final BytesReference other) {
        return compareIterators(this, other, (a, b) -> a.compareTo(b));
    }

    /**
     * Compares the two references using the given int function.
     */
    private static int compareIterators(final BytesReference a, final BytesReference b, final ToIntBiFunction<BytesRef, BytesRef> f) {
        try {
            // we use the iterators since it's a 0-copy comparison where possible!
            final long lengthToCompare = Math.min(a.length(), b.length());
            final BytesRefIterator aIter = a.iterator();
            final BytesRefIterator bIter = b.iterator();
            BytesRef aRef = aIter.next();
            BytesRef bRef = bIter.next();
            if (aRef != null && bRef != null) { // do we have any data?
                aRef = aRef.clone(); // we clone since we modify the offsets and length in the iteration below
                bRef = bRef.clone();
                if (aRef.length == a.length() && bRef.length == b.length()) { // is it only one array slice we are comparing?
                    return f.applyAsInt(aRef, bRef);
                } else {
                    for (int i = 0; i < lengthToCompare;) {
                        if (aRef.length == 0) {
                            aRef = aIter.next().clone(); // must be non null otherwise we have a bug
                        }
                        if (bRef.length == 0) {
                            bRef = bIter.next().clone(); // must be non null otherwise we have a bug
                        }
                        final int aLength = aRef.length;
                        final int bLength = bRef.length;
                        final int length = Math.min(aLength, bLength); // shrink to the same length and use the fast compare in lucene
                        aRef.length = bRef.length = length;
                        // now we move to the fast comparison - this is the hot part of the loop
                        int diff = f.applyAsInt(aRef, bRef);
                        aRef.length = aLength;
                        bRef.length = bLength;

                        if (diff != 0) {
                            return diff;
                        }
                        advance(aRef, length);
                        advance(bRef, length);
                        i += length;
                    }
                }
            }
            // One is a prefix of the other, or, they are equal:
            return a.length() - b.length();
        } catch (IOException ex) {
            throw new AssertionError("can not happen", ex);
        }
    }

    private static void advance(final BytesRef ref, final int length) {
        assert ref.length >= length : " ref.length: " + ref.length + " length: " + length;
        assert ref.offset+length < ref.bytes.length || (ref.offset+length == ref.bytes.length && ref.length-length == 0)
            : "offset: " + ref.offset + " ref.bytes.length: " + ref.bytes.length + " length: " + length + " ref.length: " + ref.length;
        ref.length -= length;
        ref.offset += length;
    }

    /**
     * Instead of adding the complexity of {@link InputStream#reset()} etc to the actual impl
     * this wrapper builds it on top of the BytesReferenceStreamInput which is much simpler
     * that way.
     */
    private static final class MarkSupportingStreamInputWrapper extends StreamInput {
        // can't use FilterStreamInput it needs to reset the delegate
        private final BytesReference reference;
        private BytesReferenceStreamInput input;
        private int mark = 0;

        private MarkSupportingStreamInputWrapper(BytesReference reference) throws IOException {
            this.reference = reference;
            this.input = new BytesReferenceStreamInput(reference.iterator(), reference.length());
        }

        @Override
        public byte readByte() throws IOException {
            return input.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            input.readBytes(b, offset, len);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return input.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        @Override
        public int read() throws IOException {
            return input.read();
        }

        @Override
        public int available() throws IOException {
            return input.available();
        }

        @Override
        protected void ensureCanReadBytes(int length) throws EOFException {
            input.ensureCanReadBytes(length);
        }

        @Override
        public void reset() throws IOException {
            input = new BytesReferenceStreamInput(reference.iterator(), reference.length());
            input.skip(mark);
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public void mark(int readLimit) {
            // readLimit is optional it only guarantees that the stream remembers data upto this limit but it can remember more
            // which we do in our case
            this.mark = input.getOffset();
        }

        @Override
        public long skip(long n) throws IOException {
            return input.skip(n);
        }
    }
}
