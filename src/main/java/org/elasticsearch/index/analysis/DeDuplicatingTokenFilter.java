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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.codecs.bloom.MurmurHash2;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.ArrayList;

public class DeDuplicatingTokenFilter extends FilteringTokenFilter {
    private final DuplicateSequenceLengthAttribute seqAtt = addAttribute(DuplicateSequenceLengthAttribute.class);
    private int minDupChainLength;

    public DeDuplicatingTokenFilter(Version version, TokenStream in, ByteStreamDuplicateSequenceSpotter byteStreamDuplicateSpotter,
            int minDupChainLength) {
        // Need to compute the chain-length to both left and right of each
        // token so need a dedup window size double that of the minDupChainLength        
        super(version, new DuplicateTaggingFilter(byteStreamDuplicateSpotter, minDupChainLength * 2, in));
        this.minDupChainLength = minDupChainLength;
    }
    
    @Override
    protected boolean accept() throws IOException {
        return seqAtt.getSequenceLength() < minDupChainLength;
    }
    
    private static class DuplicateTaggingFilter extends TokenFilter{
        private final DuplicateSequenceLengthAttribute seqAtt = addAttribute(DuplicateSequenceLengthAttribute.class);
        TermToBytesRefAttribute termBytesAtt=addAttribute(TermToBytesRefAttribute.class);
        private ByteStreamDuplicateSequenceSpotter byteStreamDuplicateSpotter;

    protected DuplicateTaggingFilter(ByteStreamDuplicateSequenceSpotter byteStreamDuplicateSpotter, int windowSize, TokenStream input) {
            super(input);
            this.byteStreamDuplicateSpotter=byteStreamDuplicateSpotter;
            this.windowSize=windowSize;
        }
        private ArrayList<State> tokens;
        private ArrayList<Boolean> valids;
        int pos=0;
        private int windowSize;



    @Override
    public final boolean incrementToken() throws IOException {
        if (tokens == null) {
            loadTokensBuffer();
        }
        clearAttributes();
        if (pos < tokens.size()) {
            State earlierToken = tokens.get(pos);
            pos++;
            restoreState(earlierToken);
            return true;
        } else {
            return false;
        }
    }
    
       

        public void loadTokensBuffer() throws IOException {
            tokens = new ArrayList<State>();
            valids = new ArrayList<Boolean>();
            pos = 0;
            boolean isWrapped = false;
            // int windowSize = 256;
            State tokenStates[] = new State[windowSize];
            int maxLens[] = new int[windowSize];
            int cursor = 0;
            BytesRef bytesRef=termBytesAtt.getBytesRef();
            while (input.incrementToken()) {
                termBytesAtt.fillBytesRef();
                int tokenHash = MurmurHash2.hash32(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                byte tokenByte = (byte) (tokenHash % 256);
                tokenStates[cursor] = captureState();
                int len = byteStreamDuplicateSpotter.addByte(tokenByte);

                // Mark prior tokens with the longest chain length of any
                // significance
                int pos = cursor;
                int numLengthsToRecord = len;
                // while (len >= container.minDupChainLength) {
                while (numLengthsToRecord > 0) {
                    if (pos < 0) {
                        pos = windowSize - 1;
                    }
                    maxLens[pos] = Math.max(maxLens[pos], len);
                    numLengthsToRecord--;
                    pos--;
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
                    if (tokenStates[cursor] != null) {
                        recordLengthInfoState(maxLens, tokenStates, cursor);
                    }
                }
            } // end loop reading all tokens from stream

            // Flush the buffered tokens
            int pos = isWrapped?nextAfter(cursor):0;
            while (pos != cursor) {
                recordLengthInfoState(maxLens, tokenStates, pos);
                pos=nextAfter(pos);
            } 
        }
        

        int nextAfter(int pos){
            pos++;
            if (pos >= windowSize) {
                pos = 0;
            }
            return pos;
        }
        
        private final void recordLengthInfoState(int[] maxLens, State[] tokenStates, int cursor) {
            if (maxLens[cursor] > 0) {
                // We need to patch in the max sequence length we recorded at
                // this position
                // into the token state
                restoreState(tokenStates[cursor]);
                seqAtt.setSequenceLength(maxLens[cursor]);
                maxLens[cursor] = 0;
                // record the patched state
                tokenStates[cursor] = captureState();
            }
            tokens.add(tokenStates[cursor]);
        }

        @Override
        public final void reset() throws IOException {
            super.reset();
            byteStreamDuplicateSpotter.reset();
            if (tokens != null) {
                tokens.clear();
                tokens = null;
            }
        }
    }
}
