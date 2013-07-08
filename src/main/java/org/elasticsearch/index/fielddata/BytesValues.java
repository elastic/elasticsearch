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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;

/**
 */
public abstract class BytesValues {

    public static final BytesValues EMPTY = new Empty();
    private boolean multiValued;
    protected final BytesRef scratch = new BytesRef();

    protected BytesValues(boolean multiValued) {
        this.multiValued = multiValued;
    }

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    public final boolean isMultiValued() {
        return multiValued;
    }

    /**
     * Is there a value for this doc?
     */
    public abstract boolean hasValue(int docId);

    /**
     * Converts the provided bytes to "safe" ones from a "non" safe call made (if needed). Note,
     * this calls makes the bytes safe for *reads*, not writes (into the same BytesRef). For example,
     * it makes it safe to be placed in a map.
     */
    public BytesRef makeSafe(BytesRef bytes) {
        return BytesRef.deepCopyOf(bytes);
    }

    /**
     * Returns a bytes value for a docId. Note, the content of it might be shared across invocation.
     */
    public BytesRef getValue(int docId) {
        if (hasValue(docId)) {
            return getValueScratch(docId, scratch);
        }
        return null;
    }

    /**
     * Returns the bytes value for the docId, with the provided "ret" which will be filled with the
     * result which will also be returned. If there is no value for this docId, the length will be 0.
     * Note, the bytes are not "safe".
     */
    public abstract BytesRef getValueScratch(int docId, BytesRef ret);


    /**
     * Fills the given spare for the given doc ID and returns the hashcode of the reference as defined by
     * {@link BytesRef#hashCode()}
     */
    public int getValueHashed(int docId, BytesRef spare) {
        return getValueScratch(docId, spare).hashCode();
    }

    /**
     * Returns a bytes value iterator for a docId. Note, the content of it might be shared across invocation.
     */
    public abstract Iter getIter(int docId); // TODO: maybe this should return null for no values so we can safe one call?


    public static interface Iter {

        boolean hasNext();

        BytesRef next();

        int hash();

        public static class Empty implements Iter {

            public static final Empty INSTANCE = new Empty();

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public BytesRef next() {
                throw new ElasticSearchIllegalStateException();
            }

            @Override
            public int hash() {
                return 0;
            }
        }

        public static class Single implements Iter {

            protected BytesRef value;
            protected long ord;
            protected boolean done;

            public Single reset(BytesRef value, long ord) {
                this.value = value;
                this.ord = ord;
                this.done = false;
                return this;
            }

            @Override
            public boolean hasNext() {
                return !done;
            }

            @Override
            public BytesRef next() {
                assert !done;
                done = true;
                return value;
            }

            public int hash() {
                return value.hashCode();
            }
        }

        static class Multi implements Iter {

            protected long innerOrd;
            protected long ord;
            protected BytesValues.WithOrdinals withOrds;
            protected Ordinals.Docs.Iter ordsIter;
            protected final BytesRef scratch = new BytesRef();

            public Multi(WithOrdinals withOrds) {
                this.withOrds = withOrds;
                assert withOrds.isMultiValued();

            }

            public Multi reset(Ordinals.Docs.Iter ordsIter) {
                this.ordsIter = ordsIter;
                innerOrd = ord = ordsIter.next();
                return this;
            }

            @Override
            public boolean hasNext() {
                return innerOrd != 0;
            }

            @Override
            public BytesRef next() {
                withOrds.getValueScratchByOrd(innerOrd, scratch);
                ord = innerOrd;
                innerOrd = ordsIter.next();
                return scratch;
            }

            public int hash() {
                return scratch.hashCode();
            }
        }
    }

    public static class Empty extends BytesValues {

        public Empty() {
            super(false);
        }

        @Override
        public boolean hasValue(int docId) {
            return false;
        }

        @Override
        public Iter getIter(int docId) {
            return Iter.Empty.INSTANCE;
        }

        @Override
        public BytesRef getValueScratch(int docId, BytesRef ret) {
            ret.length = 0;
            return ret;
        }
    }


    /**
     * Bytes values that are based on ordinals.
     */
    public static abstract class WithOrdinals extends BytesValues {

        protected final Docs ordinals;

        protected WithOrdinals(Ordinals.Docs ordinals) {
            super(ordinals.isMultiValued());
            this.ordinals = ordinals;
        }

        public Ordinals.Docs ordinals() {
            return ordinals;
        }

        public BytesRef getValueByOrd(long ord) {
            return getValueScratchByOrd(ord, scratch);
        }

        protected Iter.Multi newMultiIter() {
            assert this.isMultiValued();
            return new Iter.Multi(this);
        }

        protected Iter.Single newSingleIter() {
            assert !this.isMultiValued();
            return new Iter.Single();
        }

        @Override
        public boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != 0;
        }

        @Override
        public BytesRef getValue(int docId) {
            final long ord = ordinals.getOrd(docId);
            if (ord == 0) {
                return null;
            }
            return getValueScratchByOrd(ord, scratch);
        }

        @Override
        public BytesRef getValueScratch(int docId, BytesRef ret) {
            return getValueScratchByOrd(ordinals.getOrd(docId), ret);
        }

        public BytesRef getSafeValueByOrd(int ord) {
            return getValueScratchByOrd(ord, new BytesRef());
        }

        /**
         * Returns the bytes value for the docId, with the provided "ret" which will be filled with the
         * result which will also be returned. If there is no value for this docId, the length will be 0.
         * Note, the bytes are not "safe".
         */
        public abstract BytesRef getValueScratchByOrd(long ord, BytesRef ret);

        public static class Empty extends WithOrdinals {

            public Empty(Ordinals.Docs ordinals) {
                super(ordinals);
            }

            @Override
            public BytesRef getValueScratchByOrd(long ord, BytesRef ret) {
                ret.length = 0;
                return ret;
            }

            @Override
            public Iter getIter(int docId) {
                return Iter.Empty.INSTANCE;
            }

        }
    }
}
