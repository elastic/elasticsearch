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

import java.util.Arrays;


/**
 * Records a potentially endless stream of bytes and for each byte added
 * reports how many of the immediate predecessors were seen in that sequence
 * previously (the maximum chain length seen in a buffer of controllable size) 
 */
public final class ByteStreamDuplicateSequenceSpotter {
    
    private final int ringLength;
    private final byte[] byteBuffer;
    private final byte[] chainLengths;
    private int nextFreePos = 0;
    private int numAdditions;
    private byte lastBestChainLength = 0;    
    
    final int priorIndex(int i){
        if(i>0){
            return i-1;
        }
        return ringLength-1;
    }
    
    /**
     * Adds the next byte in a stream, returning the cumulative length of duplicate
     * bytes seen in succession. The definition of "duplicate" is dependent on the 
     * earlier sightings remaining in our sliding window of content.
     * 
     * Given an infinite window of content, the result will have no false negatives 
     * (i.e. failures to spot duplicate byte sequences) but may contain false positives 
     * (i.e. reports a sequence as duplicate due to hash collisions). The longer the 
     * sequence of tokens, the less likely a false positive will occur.
     * Given a larger window (and we typically use a large one) the number of false 
     * negatives will decrease but the time to check for duplicates will increase. 
     * 
     * TODO add option of a user-defined "slop factor" into chains e.g. allow one or 
     * more non-contiguous tokens in a chain - could somehow encode (using reserved bits?) 
     * how many slops have been encountered in a chain as we propagate counts along the 
     * chainLengths array. 
     * 
     * 
     * @param newByte The next token being processed in a stream
     * @return The longest recorded sequence of tokens seen in the current stream (max recorded value is 127)
     * 
     * TODO could still use a byte array internally for chainLength but once we are into >127 we could use
     * a single int held in instance data to keep incrementing when we can no longer count above constraints 
     * of a byte. This has the potential to misreport lengths but would be unlikely. 
     */
    byte addByte(byte newByte) {
        byte bestLength = 0;
        // Walk the entire ring backwards - if we do it forwards it messes up
        // counts being carried forward
        int i = priorIndex(nextFreePos);
        int numToCheck = Math.min(numAdditions-1, ringLength-1);
        while (numToCheck >= 0) {
            int prior = priorIndex(i);
            byte priorCount = chainLengths[prior];
            if (byteBuffer[i] == newByte) {
                byte newCount = 1;
                if (priorCount == lastBestChainLength) {
                    // Carry forward the best chain header adding one to its
                    // count (as far as a byte will let us count)
                    // A low-probability of false positives here - we don't
                    // check that newToken and its preceeding tokens are
                    // the ones that gave rise to the recorded chain end here
                    // but this is mitigated by the fact that contiguous
                    // waves of such low-probability false-positives would 
                    // need to occur at the same spot before the
                    // final min duplicate chain length (typically 8 or so
                    // tokens) is filtered. A very low risk.
                    newCount = priorCount == Byte.MAX_VALUE ? Byte.MAX_VALUE : (byte) (priorCount + 1);
                    chainLengths[i] = newCount;
                    chainLengths[prior] = 0; // clean out the count to reflect
                                             // the new head position
                } else {
                    // The newByte is not a continuation of the prior
                    // chain - establish a new chain with a count of 1 and terminate the old one
                    chainLengths[i] = newCount;
                    chainLengths[prior] = 0; // clean out the cold trail
                }
                bestLength = (byte) Math.max(bestLength, newCount);
            } else {
                if (priorCount > 0) {
                    // Our newByte is not the hoped-for continuation of a prior
                    // chain - reset the chain to mark its end and avoid further growth.
                    chainLengths[prior] = 0;
                    chainLengths[i] = 0;
                }
            }
            numToCheck--;
            i = priorIndex(i);
        }
        lastBestChainLength = bestLength;
        
        // Now add the new token to the buffer
        // trash the chain counts as we have changed the data that is being
        // counted here
        if (numAdditions >= ringLength) {
            chainLengths[nextFreePos] = 0;
        }

        if(numAdditions<Integer.MAX_VALUE){
            numAdditions++;
        }
        byteBuffer[nextFreePos] = newByte;

        nextFreePos++;
        if (nextFreePos >= ringLength) {
            nextFreePos = 0;
        }
        return bestLength;
    }

    public ByteStreamDuplicateSequenceSpotter(int numRecordedBytes) {
        ringLength=numRecordedBytes;
        byteBuffer = new byte[ringLength];
        chainLengths = new byte[ringLength];
    }

    public void reset() {
        Arrays.fill(chainLengths, 0, ringLength, (byte) 0);
        // TODO need to reserve a special byte to indicate end of doc? 
        // Would prevent matches over doc boundaries but I'm reluctant 
        // to give up one of my 256 bytes for this rare scenario 
        lastBestChainLength = 0;
    }
}
