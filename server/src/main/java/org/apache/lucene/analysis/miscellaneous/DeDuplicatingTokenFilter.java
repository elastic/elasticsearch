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

package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Inspects token streams for duplicate sequences of tokens. Token sequences
 * have a minimum length - 6 is a good heuristic as it avoids filtering common
 * idioms/phrases but detects longer sections that are typical of cut+paste
 * copies of text.
 * 
 * <p>
 * Internally each token is hashed/moduloed into a single byte (so 256 possible
 * values for each token) and then recorded in a trie of seen byte sequences
 * using a {@link DuplicateByteSequenceSpotter}. This trie is passed into the
 * TokenFilter constructor so a single object can be reused across multiple
 * documents.
 * 
 * <p>
 * The emitDuplicates setting controls if duplicate tokens are filtered from
 * results or are output (the {@link DuplicateSequenceAttribute} attribute can
 * be used to inspect the number of prior sightings when emitDuplicates is true)
 */
public class DeDuplicatingTokenFilter extends FilteringTokenFilter {
    private final DuplicateSequenceAttribute seqAtt = addAttribute(DuplicateSequenceAttribute.class);
    private final boolean emitDuplicates;
    static final MurmurHash3.Hash128 seed = new MurmurHash3.Hash128();

    public DeDuplicatingTokenFilter(TokenStream in, DuplicateByteSequenceSpotter byteStreamDuplicateSpotter) {
        this(in, byteStreamDuplicateSpotter, false);
    }

    /**
     * 
     * @param in
     *            The input token stream
     * @param byteStreamDuplicateSpotter
     *            object which retains trie of token sequences
     * @param emitDuplicates
     *            true if duplicate tokens are to be emitted (use
     *            {@link DuplicateSequenceAttribute} attribute to inspect number
     *            of prior sightings of tokens as part of a sequence).
     */
    public DeDuplicatingTokenFilter(TokenStream in, DuplicateByteSequenceSpotter byteStreamDuplicateSpotter, boolean emitDuplicates) {
        super(new DuplicateTaggingFilter(byteStreamDuplicateSpotter, in));
        this.emitDuplicates = emitDuplicates;
    }

    @Override
    protected boolean accept() throws IOException {
        return emitDuplicates || seqAtt.getNumPriorUsesInASequence() < 1;
    }

    private static class DuplicateTaggingFilter extends TokenFilter {
        private final DuplicateSequenceAttribute seqAtt = addAttribute(DuplicateSequenceAttribute.class);

        TermToBytesRefAttribute termBytesAtt = addAttribute(TermToBytesRefAttribute.class);
        private DuplicateByteSequenceSpotter byteStreamDuplicateSpotter;
        private ArrayList<State> allTokens;
        int pos = 0;
        private final int windowSize;

        protected DuplicateTaggingFilter(DuplicateByteSequenceSpotter byteStreamDuplicateSpotter, TokenStream input) {
            super(input);
            this.byteStreamDuplicateSpotter = byteStreamDuplicateSpotter;
            this.windowSize = DuplicateByteSequenceSpotter.TREE_DEPTH;
        }


        @Override
        public final boolean incrementToken() throws IOException {
            if (allTokens == null) {
                loadAllTokens();
            }
            clearAttributes();
            if (pos < allTokens.size()) {
                State earlierToken = allTokens.get(pos);
                pos++;
                restoreState(earlierToken);
                return true;
            } else {
                return false;
            }
        }

        public void loadAllTokens() throws IOException {
            // TODO consider changing this implementation to emit tokens as-we-go 
            // rather than buffering all. However this array is perhaps not the 
            // bulk of memory usage (in practice the dupSequenceSpotter requires 
            // ~5x the original content size in its internal tree ).
            allTokens = new ArrayList<State>(256);

            /*
             * Given the bytes 123456123456 and a duplicate sequence size of 6
             * the byteStreamDuplicateSpotter will only flag the final byte as
             * part of a duplicate sequence due to the byte-at-a-time streaming
             * nature of its assessments. When this happens we retain a buffer
             * of the last 6 tokens so that we can mark the states of prior
             * tokens (bytes 7 to 11) as also being duplicates
             */

            pos = 0;
            boolean isWrapped = false;
            State priorStatesBuffer[] = new State[windowSize];
            short priorMaxNumSightings[] = new short[windowSize];
            int cursor = 0;
            while (input.incrementToken()) {
                BytesRef bytesRef = termBytesAtt.getBytesRef();
                long tokenHash = MurmurHash3.hash128(bytesRef.bytes, bytesRef.offset, bytesRef.length, 0, seed).h1;
                byte tokenByte = (byte) (tokenHash & 0xFF);
                short numSightings = byteStreamDuplicateSpotter.addByte(tokenByte);
                priorStatesBuffer[cursor] = captureState();
                // Revise prior captured State objects if the latest
                // token is marked as a duplicate
                if (numSightings >= 1) {
                    int numLengthsToRecord = windowSize;
                    int pos = cursor;
                    while (numLengthsToRecord > 0) {
                        if (pos < 0) {
                            pos = windowSize - 1;
                        }
                        priorMaxNumSightings[pos] = (short) Math.max(priorMaxNumSightings[pos], numSightings);
                        numLengthsToRecord--;
                        pos--;
                    }
                }
                // Reposition cursor to next free slot
                cursor++;
                if (cursor >= windowSize) {
                    // wrap around the buffer
                    cursor = 0;
                    isWrapped = true;
                }
                // clean out the end of the tail that we may overwrite if the
                // next iteration adds a new head
                if (isWrapped) {
                    // tokenPos is now positioned on tail - emit any valid
                    // tokens we may about to overwrite in the next iteration
                    if (priorStatesBuffer[cursor] != null) {
                        recordLengthInfoState(priorMaxNumSightings, priorStatesBuffer, cursor);
                    }
                }
            } // end loop reading all tokens from stream

            // Flush the buffered tokens
            int pos = isWrapped ? nextAfter(cursor) : 0;
            while (pos != cursor) {
                recordLengthInfoState(priorMaxNumSightings, priorStatesBuffer, pos);
                pos = nextAfter(pos);
            }
        }

        private int nextAfter(int pos) {
            pos++;
            if (pos >= windowSize) {
                pos = 0;
            }
            return pos;
        }

        private void recordLengthInfoState(short[] maxNumSightings, State[] tokenStates, int cursor) {
            if (maxNumSightings[cursor] > 0) {
                // We need to patch in the max sequence length we recorded at
                // this position into the token state
                restoreState(tokenStates[cursor]);
                seqAtt.setNumPriorUsesInASequence(maxNumSightings[cursor]);
                maxNumSightings[cursor] = 0;
                // record the patched state
                tokenStates[cursor] = captureState();
            }
            allTokens.add(tokenStates[cursor]);
        }

    }
}