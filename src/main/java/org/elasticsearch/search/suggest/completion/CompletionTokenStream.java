/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 *
 */
public final class CompletionTokenStream extends TokenStream {

    private final PayloadAttribute payloadAttr = addAttribute(PayloadAttribute.class);;
    private final PositionIncrementAttribute posAttr = addAttribute(PositionIncrementAttribute.class);
    private final ByteTermAttribute bytesAtt = addAttribute(ByteTermAttribute.class);

    private final TokenStream input;
    private BytesRef payload;
    private Iterator<IntsRef> finiteStrings;
    private ToFiniteStrings toFiniteStrings;
    private int posInc = -1;
    private static final int MAX_PATHS = 256;
    private final BytesRef scratch = new BytesRef();

    public CompletionTokenStream(TokenStream input, BytesRef payload, ToFiniteStrings toFiniteStrings) throws IOException {
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
            Util.toBytesRef(finiteStrings.next(), scratch); // now we have UTF-8
            bytesAtt.setBytesRef(scratch);
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
        if (posInc == -1) {
            input.close();
        }
    }

    public static interface ToFiniteStrings {
        public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        finiteStrings = null;
        posInc = -1;
    }

    public interface ByteTermAttribute extends TermToBytesRefAttribute {
        public void setBytesRef(BytesRef bytes);
    }

    public static final class ByteTermAttributeImpl extends AttributeImpl implements ByteTermAttribute, TermToBytesRefAttribute {
        private BytesRef bytes;

        @Override
        public int fillBytesRef() {
            return bytes.hashCode();
        }

        @Override
        public BytesRef getBytesRef() {
            return bytes;
        }

        @Override
        public void setBytesRef(BytesRef bytes) {
            this.bytes = bytes;
        }

        @Override
        public void clear() {
        }

        @Override
        public void copyTo(AttributeImpl target) {
            ByteTermAttributeImpl other = (ByteTermAttributeImpl) target;
            other.bytes = bytes;
        }
    }
}