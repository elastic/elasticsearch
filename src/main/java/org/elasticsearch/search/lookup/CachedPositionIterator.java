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

package org.elasticsearch.search.lookup;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

import java.io.IOException;
import java.util.Iterator;

/*
 * Can iterate over the positions of a term an arbotrary number of times. 
 * */
public class CachedPositionIterator extends PositionIterator {

    public CachedPositionIterator(IndexFieldTerm indexFieldTerm) {
        super(indexFieldTerm);
    }

    // all payloads of the term in the current document in one bytes array.
    // payloadStarts and payloadLength mark the start and end of one payload.
    final BytesRef payloads = new BytesRef();

    final IntsRef payloadsLengths = new IntsRef(0);

    final IntsRef payloadsStarts = new IntsRef(0);

    final IntsRef positions = new IntsRef(0);

    final IntsRef startOffsets = new IntsRef(0);

    final IntsRef endOffsets = new IntsRef(0);

    @Override
    public Iterator<TermPosition> reset() {
        return new Iterator<TermPosition>() {
            private int pos = 0;
            private final TermPosition termPosition = new TermPosition();

            @Override
            public boolean hasNext() {
                return pos < freq;
            }

            @Override
            public TermPosition next() {
                termPosition.position = positions.ints[pos];
                termPosition.startOffset = startOffsets.ints[pos];
                termPosition.endOffset = endOffsets.ints[pos];
                termPosition.payload = payloads;
                payloads.offset = payloadsStarts.ints[pos];
                payloads.length = payloadsLengths.ints[pos];
                pos++;
                return termPosition;
            }

            @Override
            public void remove() {
            }
        };
    }


    private void record() throws IOException {
        TermPosition termPosition;
        for (int i = 0; i < freq; i++) {
            termPosition = super.next();
            positions.ints[i] = termPosition.position;
            addPayload(i, termPosition.payload);
            startOffsets.ints[i] = termPosition.startOffset;
            endOffsets.ints[i] = termPosition.endOffset;
        }
    }
    private void ensureSize(int freq) {
        if (freq == 0) {
            return;
        }
        if (startOffsets.ints.length < freq) {
            startOffsets.grow(freq);
            endOffsets.grow(freq);
            positions.grow(freq);
            payloadsLengths.grow(freq);
            payloadsStarts.grow(freq);
        }
        payloads.offset = 0;
        payloadsLengths.offset = 0;
        payloadsStarts.offset = 0;
        payloads.grow(freq * 8);// this is just a guess....

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


    @Override
    public void nextDoc() throws IOException {
        super.nextDoc();
        ensureSize(freq);
        record();
    }

    @Override
    public TermPosition next() {
        throw new UnsupportedOperationException();
    }
}
