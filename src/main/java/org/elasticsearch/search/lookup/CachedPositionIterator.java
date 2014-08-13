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
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRefBuilder;

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
    final BytesRefBuilder payloads = new BytesRefBuilder();

    final IntsRefBuilder payloadsLengths = new IntsRefBuilder();

    final IntsRefBuilder payloadsStarts = new IntsRefBuilder();

    final IntsRefBuilder positions = new IntsRefBuilder();

    final IntsRefBuilder startOffsets = new IntsRefBuilder();

    final IntsRefBuilder endOffsets = new IntsRefBuilder();

    final BytesRef payload = new BytesRef();

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
                termPosition.position = positions.intAt(pos);
                termPosition.startOffset = startOffsets.intAt(pos);
                termPosition.endOffset = endOffsets.intAt(pos);
                termPosition.payload = payload;
                payload.bytes = payloads.bytes();
                payload.offset = payloadsStarts.intAt(pos);
                payload.length = payloadsLengths.intAt(pos);
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
            positions.setIntAt(i, termPosition.position);
            addPayload(i, termPosition.payload);
            startOffsets.setIntAt(i, termPosition.startOffset);
            endOffsets.setIntAt(i, termPosition.endOffset);
        }
    }
    private void ensureSize(int freq) {
        if (freq == 0) {
            return;
        }
        startOffsets.grow(freq);
        endOffsets.grow(freq);
        positions.grow(freq);
        payloadsLengths.grow(freq);
        payloadsStarts.grow(freq);
        payloads.grow(freq * 8);// this is just a guess....

    }

    private void addPayload(int i, BytesRef currPayload) {
        if (currPayload != null) {
            payloadsLengths.setIntAt(i, currPayload.length);
            payloadsStarts.setIntAt(i, i == 0 ? 0 : payloadsStarts.intAt(i - 1) + payloadsLengths.intAt(i - 1));
            payloads.grow(payloadsStarts.intAt(i) + currPayload.length);
            System.arraycopy(currPayload.bytes, currPayload.offset, payloads.bytes(), payloadsStarts.intAt(i), currPayload.length);
        } else {
            payloadsLengths.setIntAt(i, 0);
            payloadsStarts.setIntAt(i, i == 0 ? 0 : payloadsStarts.intAt(i - 1) + payloadsLengths.intAt(i - 1));
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
