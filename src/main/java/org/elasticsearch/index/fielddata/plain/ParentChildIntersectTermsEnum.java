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

package org.elasticsearch.index.fielddata.plain;

import com.carrotsearch.hppc.IntArrayList;

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Intersects the terms and unions the doc ids for terms enum of multiple fields.
 *
 * @elasticsearch.internal
 */
final class ParentChildIntersectTermsEnum extends TermsEnum {

    private final List<TermsEnumState> states;
    private final IntArrayList stateSlots;

    private BytesRef current;

    ParentChildIntersectTermsEnum(LeafReader atomicReader, String... fields) throws IOException {
        List<TermsEnum> fieldEnums = new ArrayList<>();
        for (String field : fields) {
            Terms terms = atomicReader.terms(field);
            if (terms != null) {
                fieldEnums.add(terms.iterator());
            }
        }
        states = new ArrayList<>(fieldEnums.size());
        for (TermsEnum tEnum : fieldEnums) {
            states.add(new TermsEnumState(tEnum));
        }
        stateSlots = new IntArrayList(states.size());
    }

    @Override
    public BytesRef term() throws IOException {
        return current;
    }

    @Override
    public PostingsEnum postings(Bits liveDocs, PostingsEnum reuse, int flags) throws IOException {
        int size = stateSlots.size();
        assert size > 0;
        if (size == 1) {
            // Can't use 'reuse' since we don't know to which previous TermsEnum it belonged to.
            return states.get(stateSlots.get(0)).termsEnum.postings(liveDocs, null, flags);
        } else {
            List<PostingsEnum> docsEnums = new ArrayList<>(stateSlots.size());
            for (int i = 0; i < stateSlots.size(); i++) {
                docsEnums.add(states.get(stateSlots.get(i)).termsEnum.postings(liveDocs, null, flags));
            }
            return new CompoundDocsEnum(docsEnums);
        }
    }

    @Override
    public BytesRef next() throws IOException {
        if (states.isEmpty()) {
            return null;
        }

        if (current == null) {
            // unpositioned
            for (TermsEnumState state : states) {
                state.initialize();
            }
        } else {
            int removed = 0;
            for (int i = 0; i < stateSlots.size(); i++) {
                int stateSlot = stateSlots.get(i);
                if (states.get(stateSlot - removed).next() == null) {
                    states.remove(stateSlot - removed);
                    removed++;
                }
            }

            if (states.isEmpty()) {
                return null;
            }
            stateSlots.clear();
        }

        BytesRef lowestTerm = states.get(0).term;
        stateSlots.add(0);
        for (int i = 1; i < states.size(); i++) {
            TermsEnumState state = states.get(i);
            int cmp = lowestTerm.compareTo(state.term);
            if (cmp > 0) {
                lowestTerm = state.term;
                stateSlots.clear();
                stateSlots.add(i);
            } else if (cmp == 0) {
                stateSlots.add(i);
            }
        }

        return current = lowestTerm;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
        if (states.isEmpty()) {
            return SeekStatus.END;
        }

        boolean found  = false;
        if (current == null) {
            // unpositioned
            Iterator<TermsEnumState> iterator = states.iterator();
            while (iterator.hasNext()) {
                SeekStatus seekStatus = iterator.next().seekCeil(text);
                if (seekStatus == SeekStatus.END) {
                    iterator.remove();
                } else if (seekStatus == SeekStatus.FOUND) {
                    found = true;
                }
            }
        } else {
            int removed = 0;
            for (int i = 0; i < stateSlots.size(); i++) {
                int stateSlot = stateSlots.get(i);
                SeekStatus seekStatus = states.get(stateSlot - removed).seekCeil(text);
                if (seekStatus == SeekStatus.END) {
                    states.remove(stateSlot - removed);
                    removed++;
                } else if (seekStatus == SeekStatus.FOUND) {
                    found = true;
                }
            }
        }

        if (states.isEmpty()) {
            return SeekStatus.END;
        }
        stateSlots.clear();

        if (found) {
            for (int i = 0; i < states.size(); i++) {
                if (states.get(i).term.equals(text)) {
                    stateSlots.add(i);
                }
            }
            current = text;
            return SeekStatus.FOUND;
        } else {
            BytesRef lowestTerm = states.get(0).term;
            stateSlots.add(0);
            for (int i = 1; i < states.size(); i++) {
                TermsEnumState state = states.get(i);
                int cmp = lowestTerm.compareTo(state.term);
                if (cmp > 0) {
                    lowestTerm = state.term;
                    stateSlots.clear();
                    stateSlots.add(i);
                } else if (cmp == 0) {
                    stateSlots.add(i);
                }
            }
            current = lowestTerm;
            return SeekStatus.NOT_FOUND;
        }
    }

    class TermsEnumState {

        final TermsEnum termsEnum;
        BytesRef term;
        SeekStatus lastSeekStatus;

        TermsEnumState(TermsEnum termsEnum) {
            this.termsEnum = termsEnum;
        }

        void initialize() throws IOException {
            term = termsEnum.next();
        }

        BytesRef next() throws IOException {
            return term = termsEnum.next();
        }

        SeekStatus seekCeil(BytesRef text) throws IOException {
            lastSeekStatus = termsEnum.seekCeil(text);
            if (lastSeekStatus != SeekStatus.END) {
                term = termsEnum.term();
            }
            return lastSeekStatus;
        }
    }

    class CompoundDocsEnum extends PostingsEnum {

        final List<State> states;
        int current = -1;

        CompoundDocsEnum(List<PostingsEnum> docsEnums) {
            this.states = new ArrayList<>(docsEnums.size());
            for (PostingsEnum docsEnum : docsEnums) {
                states.add(new State(docsEnum));
            }
        }

        @Override
        public int freq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docID() {
            return current;
        }

        @Override
        public int nextDoc() throws IOException {
            if (states.isEmpty()) {
                return current = NO_MORE_DOCS;
            }

            if (current == -1) {
                for (State state : states) {
                    state.initialize();
                }
            }

            int lowestIndex = 0;
            int lowestDocId = states.get(0).current;
            for (int i = 1; i < states.size(); i++) {
                State state = states.get(i);
                if (lowestDocId > state.current) {
                    lowestDocId = state.current;
                    lowestIndex = i;
                }
            }

            if (states.get(lowestIndex).next() == DocIdSetIterator.NO_MORE_DOCS) {
                states.remove(lowestIndex);
            }

            return current = lowestDocId;
        }

        @Override
        public int advance(int target) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int endOffset() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef getPayload() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int nextPosition() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int startOffset() throws IOException {
            throw new UnsupportedOperationException();
        }

        class State {

            final PostingsEnum docsEnum;
            int current = -1;

            State(PostingsEnum docsEnum) {
                this.docsEnum = docsEnum;
            }

            void initialize() throws IOException {
                current = docsEnum.nextDoc();
            }

            int next() throws IOException {
                return current = docsEnum.nextDoc();
            }
        }
    }

    @Override
    public long ord() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() throws IOException {
        throw new UnsupportedOperationException();
    }
}
