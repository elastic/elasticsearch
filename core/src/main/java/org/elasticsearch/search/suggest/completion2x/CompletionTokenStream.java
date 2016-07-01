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
package org.elasticsearch.search.suggest.completion2x;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 *
 */
public final class CompletionTokenStream extends TokenStream {

    private final PayloadAttribute payloadAttr = addAttribute(PayloadAttribute.class);
    private final PositionIncrementAttribute posAttr = addAttribute(PositionIncrementAttribute.class);
    private final ByteTermAttribute bytesAtt = addAttribute(ByteTermAttribute.class);;


    private final TokenStream input;
    private BytesRef payload;
    private Iterator<IntsRef> finiteStrings;
    private ToFiniteStrings toFiniteStrings;
    private int posInc = -1;
    private static final int MAX_PATHS = 256;
    private CharTermAttribute charTermAttribute;

    public CompletionTokenStream(TokenStream input, BytesRef payload, ToFiniteStrings toFiniteStrings) {
        // Don't call the super(input) ctor - this is a true delegate and has a new attribute source since we consume
        // the input stream entirely in toFiniteStrings(input)
        this.input = input;
        this.payload = payload;
        this.toFiniteStrings = toFiniteStrings;
    }

    @Override
    public boolean incrementToken() throws IOException {
        clearAttributes();
        if (finiteStrings == null) {
            Set<IntsRef> strings = toFiniteStrings.toFiniteStrings(input);

            if (strings.size() > MAX_PATHS) {
                throw new IllegalArgumentException("TokenStream expanded to " + strings.size() + " finite strings. Only <= " + MAX_PATHS
                        + " finite strings are supported");
            }
            posInc = strings.size();
            finiteStrings = strings.iterator();
        }
        if (finiteStrings.hasNext()) {
            posAttr.setPositionIncrement(posInc);
            /*
             * this posInc encodes the number of paths that this surface form
             * produced. Multi Fields have the same surface form and therefore sum up
             */
            posInc = 0;
            Util.toBytesRef(finiteStrings.next(), bytesAtt.builder()); // now we have UTF-8
            if (charTermAttribute != null) {
                charTermAttribute.setLength(0);
                charTermAttribute.append(bytesAtt.toUTF16());
            }
            if (payload != null) {
                payloadAttr.setPayload(this.payload);
            }
            return true;
        }

        return false;
    }

    @Override
    public void end() throws IOException {
        super.end();
        if (posInc == -1) {
            input.end();
        }
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    public static interface ToFiniteStrings {
        public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        if (hasAttribute(CharTermAttribute.class)) {
            // we only create this if we really need it to safe the UTF-8 to UTF-16 conversion
            charTermAttribute = getAttribute(CharTermAttribute.class);
        }
        finiteStrings = null;
        posInc = -1;
    }

    public interface ByteTermAttribute extends TermToBytesRefAttribute {
        // marker interface

        /**
         * Return the builder from which the term is derived.
         */
        public BytesRefBuilder builder();

        public CharSequence toUTF16();
    }

    public static final class ByteTermAttributeImpl extends AttributeImpl implements ByteTermAttribute, TermToBytesRefAttribute {
        private final BytesRefBuilder bytes = new BytesRefBuilder();
        private CharsRefBuilder charsRef;

        @Override
        public BytesRefBuilder builder() {
            return bytes;
        }

        @Override
        public BytesRef getBytesRef() {
            return bytes.get();
        }

        @Override
        public void clear() {
            bytes.clear();
        }

        @Override
        public void reflectWith(AttributeReflector reflector) {

        }

        @Override
        public void copyTo(AttributeImpl target) {
            ByteTermAttributeImpl other = (ByteTermAttributeImpl) target;
            other.bytes.copyBytes(bytes);
        }

        @Override
        public CharSequence toUTF16() {
            if (charsRef == null) {
                charsRef = new CharsRefBuilder();
            }
            charsRef.copyUTF8Bytes(getBytesRef());
            return charsRef.get();
        }
    }
}
