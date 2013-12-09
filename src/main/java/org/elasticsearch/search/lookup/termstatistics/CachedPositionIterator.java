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

package org.elasticsearch.search.lookup.termstatistics;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

import java.io.IOException;
import java.util.Iterator;

/*
 * Can iterate over the positions of a term an arbotrary number of times. 
 * */
public class CachedPositionIterator extends UncachedPositionIterator {

    public CachedPositionIterator(ScriptTerm termInfo) {
        super(termInfo);
        uncached = new UncachedPositionIterator(termInfo);
    }

    // all payloads of the term in the current document in one bytes array.
    // payloadStarts and payloadLength mark the start and end of one payload.
    final BytesRef payloads = new BytesRef();

    final IntsRef payloadsLengths = new IntsRef(0);

    final IntsRef payloadsStarts = new IntsRef(0);

    final IntsRef positions = new IntsRef(0);

    final IntsRef startOffsets = new IntsRef(0);

    final IntsRef endOffsets = new IntsRef(0);

    final UncachedPositionIterator uncached;

    @Override
    public Iterator<TermPosition> reset() {
        curIteratorPos = 0;
        return this;
    }

    @Override
    protected void initTermPosition(DocsAndPositionsEnum docsAndPos) throws IOException {

        if (shouldRetrievePositions()) {
            termPosition.position = positions.ints[curIteratorPos];
        }
        if (shouldRetrieveOffsets()) {
            termPosition.startOffset = startOffsets.ints[curIteratorPos];
            termPosition.endOffset = endOffsets.ints[curIteratorPos];
        }
        if (shouldRetrievePayloads()) {
            termPosition.payload = payloads;
            payloads.offset = payloadsStarts.ints[curIteratorPos];
            payloads.length = payloadsLengths.ints[curIteratorPos];
        }

    }

    private void record() throws IOException {
        uncached.init();
        TermPosition termPosition;
        initMemory();
        for (int i = 0; i < freq; i++) {
            termPosition = uncached.next();
            if (shouldRetrievePositions()) {
                positions.ints[i] = termPosition.position;
            }
            if (shouldRetrievePayloads()) {
                addPayload(i, termPosition.payload);
            }
            if (shouldRetrieveOffsets()) {
                startOffsets.ints[i] = termPosition.startOffset;
                endOffsets.ints[i] = termPosition.endOffset;
            }
        }
    }

    private void initMemory() {
        initPosMem(freq);
        initPayloadsMem(freq);
        initOffsetsMem(freq);
    }

    @Override
    protected void initDocsAndPos() throws IOException {
        super.initDocsAndPos();
        uncached.initDocsAndPos();
    }

    private void initOffsetsMem(int freq) {
        startOffsets.grow(freq);
        endOffsets.grow(freq);
    }

    private void initPosMem(int freq) {
        positions.grow(freq);
    }

    private void initPayloadsMem(int freq) {
        payloads.offset = 0;
        payloadsLengths.offset = 0;
        payloadsStarts.offset = 0;
        payloads.grow(freq * 8);// this is just a guess....
        payloadsLengths.grow(freq);
        payloadsStarts.grow(freq);
    }

    private void addPayload(int i, BytesRef currPayload) {
        if (currPayload != null) {
            payloadsLengths.ints[i] = currPayload.length;
            payloadsStarts.ints[i] = i == 0 ? 0 : payloadsStarts.ints[i - 1] + payloadsLengths.ints[i - 1];
            if (payloads.bytes.length < payloadsStarts.ints[i] + payloadsLengths.ints[i]) {
                payloads.offset = 0; // the offset serves no purpose here. but
                                     // we must assure that it is 0 before
                                     // grow() is called
                payloads.grow(payloads.bytes.length * 2); // just a guess
            }
            System.arraycopy(currPayload.bytes, currPayload.offset, payloads.bytes, payloadsStarts.ints[i], currPayload.length);
        } else {
            payloadsLengths.ints[i] = 0;
            payloadsStarts.ints[i] = i == 0 ? 0 : payloadsStarts.ints[i - 1] + payloadsLengths.ints[i - 1];
        }
    }

    /*
     * Must be called when moving to a new document.
     */
    @Override
    protected void init() throws IOException {
        freq = scriptTerm.tf();
        curIteratorPos = 0;
        record();
    }
}
