/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.plain.FSTPackedBytesAtomicFieldData.BytesValues;

/**
 * shared utils class - should be factored into HashedBytesValues
 */
abstract class HashedBytesValuesWithOrds implements org.elasticsearch.index.fielddata.HashedBytesValues.WithOrdinals {

    protected final int[] hashes;
    protected final Ordinals.Docs ordinals;

    protected final BytesRef scratch1 = new BytesRef();
    protected final HashedBytesRef scratch = new HashedBytesRef();
    protected final BytesValues.WithOrdinals withOrds;

    HashedBytesValuesWithOrds(BytesValues.WithOrdinals withOrds, int[] hashes) {
        this.hashes = hashes;
        this.ordinals = withOrds.ordinals();
        this.withOrds = withOrds;
    }
    
    @Override
    public boolean isMultiValued() {
        return withOrds.isMultiValued();
    }
    
    @Override
    public void forEachValueInDoc(int docId, ValueInDocProc proc) {
        int ord = ordinals.getOrd(docId);
        if (ord == 0) {
            proc.onMissing(docId);
        } else {
            proc.onValue(docId, scratch.reset(withOrds.getValueScratchByOrd(ord, scratch1), hashes[ord]));
        }       
    }


    protected final void forEachValueInDocMulti(int docId, ValueInDocProc proc) {
        Ordinals.Docs.Iter iter = ordinals.getIter(docId);
        int ord = iter.next();
        if (ord == 0) {
            proc.onMissing(docId);
            return;
        }
        do {
            proc.onValue(docId, scratch.reset(withOrds.getValueScratchByOrd(ord, scratch1), hashes[ord]));
        } while ((ord = iter.next()) != 0);            
    }



    @Override
    public Ordinals.Docs ordinals() {
        return this.ordinals;
    }

    @Override
    public HashedBytesRef getValueByOrd(int ord) {
        return scratch.reset(withOrds.getValueScratchByOrd(ord, scratch1), hashes[ord]);
    }
    
    @Override
    public HashedBytesRef getSafeValueByOrd(int ord) {
        return new HashedBytesRef(withOrds.getValueScratchByOrd(ord, scratch1), hashes[ord]);
    }

    @Override
    public boolean hasValue(int docId) {
        return ordinals.getOrd(docId) != 0;
    }

    @Override
    public HashedBytesRef makeSafe(HashedBytesRef bytes) {
        return new HashedBytesRef(BytesRef.deepCopyOf(bytes.bytes), bytes.hash);
    }

    @Override
    public HashedBytesRef getValue(int docId) {
        int ord = ordinals.getOrd(docId);
        if (ord == 0) return null;
        return scratch.reset(withOrds.getValueScratchByOrd(ord, scratch1), hashes[ord]);
    }

    final static class Single extends HashedBytesValuesWithOrds {

        private final Iter.Single iter = new Iter.Single();

        Single(BytesValues.WithOrdinals withOrds, int[] hashes) {
            super(withOrds, hashes);
        }

        @Override
        public Iter getIter(int docId) {
            int ord = ordinals.getOrd(docId);
            if (ord == 0) return Iter.Empty.INSTANCE;
            return iter.reset(scratch.reset(withOrds.getValueScratchByOrd(ord, scratch1), hashes[ord]));
        }

    }

    final static class Multi extends HashedBytesValuesWithOrds {
        private final HashedBytesValuesWithOrds.Multi.MultiIter iter;

        Multi(BytesValues.WithOrdinals withOrds, int[] hashes) {
            super(withOrds, hashes);
            this.iter = new MultiIter(withOrds, hashes);
        }

        @Override
        public boolean isMultiValued() {
            return true;
        }

        @Override
        public Iter getIter(int docId) {
            return iter.reset(ordinals.getIter(docId));
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
           forEachValueInDocMulti(docId, proc);
        }

        final static class MultiIter implements Iter {

            private final int[] hashes;
            private Ordinals.Docs.Iter ordsIter;
            private int ord;
            private final BytesRef scratch1 = new BytesRef();
            private final HashedBytesRef scratch = new HashedBytesRef();
            private final BytesValues.WithOrdinals withOrds;

            MultiIter(BytesValues.WithOrdinals withOrds, int[] hashes) {
                this.hashes = hashes;
                this.withOrds = withOrds;
            }

            public HashedBytesValuesWithOrds.Multi.MultiIter reset(Ordinals.Docs.Iter ordsIter) {
                this.ordsIter = ordsIter;
                this.ord = ordsIter.next();
                return this;
            }

            @Override
            public boolean hasNext() {
                return ord != 0;
            }

            @Override
            public HashedBytesRef next() {
                HashedBytesRef value = scratch.reset(withOrds.getValueScratchByOrd(ord, scratch1), hashes[ord]);
                ord = ordsIter.next();
                return value;
            }
        }
    }
}