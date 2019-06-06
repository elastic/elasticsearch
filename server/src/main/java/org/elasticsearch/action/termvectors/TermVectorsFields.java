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

package org.elasticsearch.action.termvectors;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.apache.lucene.util.ArrayUtil.grow;

/**
 * This class represents the result of a {@link TermVectorsRequest}. It works
 * exactly like the {@link Fields} class except for one thing: It can return
 * offsets and payloads even if positions are not present. You must call
 * nextPosition() anyway to move the counter although this method only returns
 * {@code -1,}, if no positions were returned by the {@link TermVectorsRequest}.
 * <p>
 * The data is stored in two byte arrays ({@code headerRef} and
 * {@code termVectors}, both {@link BytesRef}) that have the following format:
 * <p>
 * {@code headerRef}: Stores offsets per field in the {@code termVectors} array
 * and some header information as {@link BytesRef}. Format is
 * <ul>
 * <li>String : "TV"</li>
 * <li>vint: version (=-1)</li>
 * <li>boolean: hasTermStatistics (are the term statistics stored?)</li>
 * <li>boolean: hasFieldStatitsics (are the field statistics stored?)</li>
 * <li>vint: number of fields</li>
 * <li>
 * <ul>
 * <li>String: field name 1</li>
 * <li>vint: offset in {@code termVectors} for field 1</li>
 * <li>...</li>
 * <li>String: field name last field</li>
 * <li>vint: offset in {@code termVectors} for last field</li>
 * </ul>
 * </li>
 * </ul>
 * <p>
 * termVectors: Stores the actual term vectors as a {@link BytesRef}.
 * <p>
 * Term vectors for each fields are stored in blocks, one for each field. The
 * offsets in {@code headerRef} are used to find where the block for a field
 * starts. Each block begins with a
 * <ul>
 * <li>vint: number of terms</li>
 * <li>boolean: positions (has it positions stored?)</li>
 * <li>boolean: offsets (has it offsets stored?)</li>
 * <li>boolean: payloads (has it payloads stored?)</li>
 * </ul>
 * If the field statistics were requested ({@code hasFieldStatistics} is true,
 * see {@code headerRef}), the following numbers are stored:
 * <ul>
 * <li>vlong: sum of total term frequencies of the field (sumTotalTermFreq)</li>
 * <li>vlong: sum of document frequencies for each term (sumDocFreq)</li>
 * <li>vint: number of documents in the shard that has an entry for this field
 * (docCount)</li>
 * </ul>
 * <p>
 * After that, for each term it stores
 * <ul>
 * <li>vint: term lengths</li>
 * <li>BytesRef: term name</li>
 * </ul>
 * <p>
 * If term statistics are requested ({@code hasTermStatistics} is true, see
 * {@code headerRef}):
 * <ul>
 * <li>vint: document frequency, how often does this term appear in documents?</li>
 * <li>vlong: total term frequency. Sum of terms in this field.</li>
 * </ul>
 * After that
 * <ul>
 * <li>vint: frequency (always returned)</li>
 * <li>
 * <ul>
 * <li>vint: position_1 (if positions)</li>
 * <li>vint: startOffset_1 (if offset)</li>
 * <li>vint: endOffset_1 (if offset)</li>
 * <li>BytesRef: payload_1 (if payloads)</li>
 * <li>...</li>
 * <li>vint: endOffset_freqency (if offset)</li>
 * <li>BytesRef: payload_freqency (if payloads)</li>
 * </ul></li>
 * </ul>
 */

public final class TermVectorsFields extends Fields {

    private final ObjectLongHashMap<String> fieldMap;
    private final BytesReference termVectors;
    final boolean hasTermStatistic;
    final boolean hasFieldStatistic;
    public final boolean hasScores;

    /**
     * @param headerRef   Stores offsets per field in the {@code termVectors} and some
     *                    header information as {@link BytesRef}.
     * @param termVectors Stores the actual term vectors as a {@link BytesRef}.
     */
    public TermVectorsFields(BytesReference headerRef, BytesReference termVectors) throws IOException {
        try (StreamInput header = headerRef.streamInput()) {
            fieldMap = new ObjectLongHashMap<>();
            // here we read the header to fill the field offset map
            String headerString = header.readString();
            assert headerString.equals("TV");
            int version = header.readInt();
            assert version == -1;
            hasTermStatistic = header.readBoolean();
            hasFieldStatistic = header.readBoolean();
            hasScores = header.readBoolean();
            final int numFields = header.readVInt();
            for (int i = 0; i < numFields; i++) {
                fieldMap.put((header.readString()), header.readVLong());
            }
        }
        // reference to the term vector data
        this.termVectors = termVectors;
    }

    @Override
    public Iterator<String> iterator() {
        final Iterator<ObjectLongCursor<String>> iterator = fieldMap.iterator();
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public String next() {
                return iterator.next().key;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Terms terms(String field) throws IOException {
        // first, find where in the termVectors bytes the actual term vector for
        // this field is stored
        final int keySlot = fieldMap.indexOf(field);
        if (keySlot < 0) {
            return null; // we don't have it.
        }
        long readOffset = fieldMap.indexGet(keySlot);
        return new TermVector(termVectors, readOffset);
    }

    @Override
    public int size() {
        return fieldMap.size();
    }

    private final class TermVector extends Terms {

        private final StreamInput perFieldTermVectorInput;
        private final long readOffset;

        private long numTerms;
        private boolean hasPositions;
        private boolean hasOffsets;
        private boolean hasPayloads;
        private long sumTotalTermFreq;
        private long sumDocFreq;
        private int docCount;

        TermVector(BytesReference termVectors, long readOffset) throws IOException {
            this.perFieldTermVectorInput = termVectors.streamInput();
            this.readOffset = readOffset;
            reset();
        }

        private void reset() throws IOException {
            this.perFieldTermVectorInput.reset();
            this.perFieldTermVectorInput.skip(readOffset);

            // read how many terms....
            this.numTerms = perFieldTermVectorInput.readVLong();
            // ...if positions etc. were stored....
            this.hasPositions = perFieldTermVectorInput.readBoolean();
            this.hasOffsets = perFieldTermVectorInput.readBoolean();
            this.hasPayloads = perFieldTermVectorInput.readBoolean();
            // read the field statistics
            this.sumTotalTermFreq = hasFieldStatistic ? readPotentiallyNegativeVLong(perFieldTermVectorInput) : -1;
            this.sumDocFreq = hasFieldStatistic ? readPotentiallyNegativeVLong(perFieldTermVectorInput) : -1;
            this.docCount = hasFieldStatistic ? readPotentiallyNegativeVInt(perFieldTermVectorInput) : -1;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            // reset before asking for an iterator
            reset();
            // convert bytes ref for the terms to actual data
            return new BaseTermsEnum() {
                int currentTerm = 0;
                int freq = 0;
                int docFreq = -1;
                long totalTermFrequency = -1;
                int[] positions = new int[1];
                int[] startOffsets = new int[1];
                int[] endOffsets = new int[1];
                BytesRefBuilder[] payloads = new BytesRefBuilder[1];
                final BytesRefBuilder spare = new BytesRefBuilder();
                BoostAttribute boostAtt = this.attributes().addAttribute(BoostAttribute.class);

                @Override
                public BytesRef next() throws IOException {
                    if (currentTerm++ < numTerms) {
                        // term string. first the size...
                        int termVectorSize = perFieldTermVectorInput.readVInt();
                        spare.grow(termVectorSize);
                        // ...then the value.
                        perFieldTermVectorInput.readBytes(spare.bytes(), 0, termVectorSize);
                        spare.setLength(termVectorSize);
                        if (hasTermStatistic) {
                            docFreq = readPotentiallyNegativeVInt(perFieldTermVectorInput);
                            totalTermFrequency = readPotentiallyNegativeVLong(perFieldTermVectorInput);
                        }

                        freq = readPotentiallyNegativeVInt(perFieldTermVectorInput);
                        // grow the arrays to read the values. this is just
                        // for performance reasons. Re-use memory instead of
                        // realloc.
                        growBuffers();
                        // finally, read the values into the arrays
                        // currentPosition etc. so that we can just iterate
                        // later
                        writeInfos(perFieldTermVectorInput);

                        // read the score if available
                        if (hasScores) {
                            boostAtt.setBoost(perFieldTermVectorInput.readFloat());
                        }
                        return spare.get();

                    } else {
                        return null;
                    }
                }

                private void writeInfos(final StreamInput input) throws IOException {
                    for (int i = 0; i < freq; i++) {
                        if (hasPositions) {
                            positions[i] = input.readVInt();
                        }
                        if (hasOffsets) {
                            startOffsets[i] = input.readVInt();
                            endOffsets[i] = input.readVInt();
                        }
                        if (hasPayloads) {
                            int payloadLength = input.readVInt();
                            if (payloads[i] == null) {
                                payloads[i] = new BytesRefBuilder();
                            }
                            payloads[i].grow(payloadLength);
                            input.readBytes(payloads[i].bytes(), 0, payloadLength);
                            payloads[i].setLength(payloadLength);
                        }
                    }
                }

                private void growBuffers() {
                    if (hasPositions) {
                        positions = grow(positions, freq);
                    }
                    if (hasOffsets) {
                        startOffsets = grow(startOffsets, freq);
                        endOffsets = grow(endOffsets, freq);
                    }
                    if (hasPayloads) {
                        if (payloads.length < freq) {
                            payloads = Arrays.copyOf(payloads, ArrayUtil.oversize(freq, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
                        }
                    }
                }

                @Override
                public SeekStatus seekCeil(BytesRef text) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void seekExact(long ord) throws IOException {
                    throw new UnsupportedOperationException("Seek is not supported");
                }

                @Override
                public BytesRef term() throws IOException {
                    return spare.get();
                }

                @Override
                public long ord() throws IOException {
                    throw new UnsupportedOperationException("ordinals are not supported");
                }

                @Override
                public int docFreq() throws IOException {
                    return docFreq;
                }

                @Override
                public long totalTermFreq() throws IOException {
                    return totalTermFrequency;
                }

                @Override
                public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                    final TermVectorPostingsEnum retVal = (reuse instanceof TermVectorPostingsEnum ? (TermVectorPostingsEnum) reuse
                            : new TermVectorPostingsEnum());
                    return retVal.reset(hasPositions ? positions : null, hasOffsets ? startOffsets : null, hasOffsets ? endOffsets
                            : null, hasPayloads ? payloads : null, freq);
                }

                @Override
                public ImpactsEnum impacts(int flags) throws IOException {
                    return new SlowImpactsEnum(postings(null, flags));
                }

            };
        }

        @Override
        public long size() throws IOException {
            return numTerms;
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            return sumTotalTermFreq;
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return sumDocFreq;
        }

        @Override
        public int getDocCount() throws IOException {
            return docCount;
        }

        @Override
        public boolean hasFreqs() {
            return true;
        }

        @Override
        public boolean hasOffsets() {
            return hasOffsets;
        }

        @Override
        public boolean hasPositions() {
            return hasPositions;
        }

        @Override
        public boolean hasPayloads() {
            return hasPayloads;
        }
    }

    private final class TermVectorPostingsEnum extends PostingsEnum {
        private boolean hasPositions;
        private boolean hasOffsets;
        private boolean hasPayloads;
        int curPos = -1;
        int doc = -1;
        private int freq;
        private int[] startOffsets;
        private int[] positions;
        private BytesRefBuilder[] payloads;
        private int[] endOffsets;

        private PostingsEnum reset(int[] positions, int[] startOffsets, int[] endOffsets, BytesRefBuilder[] payloads, int freq) {
            curPos = -1;
            doc = -1;
            this.hasPositions = positions != null;
            this.hasOffsets = startOffsets != null;
            this.hasPayloads = payloads != null;
            this.freq = freq;
            this.startOffsets = startOffsets;
            this.endOffsets = endOffsets;
            this.payloads = payloads;
            this.positions = positions;
            return this;
        }

        @Override
        public int nextDoc() throws IOException {
            return doc = (doc == -1 ? 0 : NO_MORE_DOCS);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            while (nextDoc() < target && doc != NO_MORE_DOCS) {
            }
            return doc;
        }

        @Override
        public int freq() throws IOException {
            return freq;
        }

        // call nextPosition once before calling this one
        // because else counter is not advanced
        @Override
        public int startOffset() throws IOException {
            assert curPos < freq && curPos >= 0;
            return hasOffsets ? startOffsets[curPos] : -1;

        }

        @Override
        // can return -1 if posistions were not requested or
        // stored but offsets were stored and requested
        public int nextPosition() throws IOException {
            assert curPos + 1 < freq;
            ++curPos;
            // this is kind of cheating but if you don't need positions
            // we safe lots fo space on the wire
            return hasPositions ? positions[curPos] : -1;
        }

        @Override
        public BytesRef getPayload() throws IOException {
            assert curPos < freq && curPos >= 0;
            if (hasPayloads) {
                final BytesRefBuilder payload = payloads[curPos];
                if (payload != null) {
                    return payload.get();
                }
            }
            return null;
        }

        @Override
        public int endOffset() throws IOException {
            assert curPos < freq && curPos >= 0;
            return hasOffsets ? endOffsets[curPos] : -1;
        }

        @Override
        public long cost() {
            return 1;
        }
    }

    // read a vInt. this is used if the integer might be negative. In this case,
    // the writer writes a 0 for -1 or value +1 and accordingly we have to
    // subtract 1 again
    // adds one to mock not existing term freq
    int readPotentiallyNegativeVInt(StreamInput stream) throws IOException {
        return stream.readVInt() - 1;
    }

    // read a vLong. this is used if the integer might be negative. In this
    // case, the writer writes a 0 for -1 or value +1 and accordingly we have to
    // subtract 1 again
    // adds one to mock not existing term freq
    long readPotentiallyNegativeVLong(StreamInput stream) throws IOException {
        return stream.readVLong() - 1;
    }
}
