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

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;

/**
 */
public abstract class LongValues {

    public static final LongValues EMPTY = new Empty();
    private final boolean multiValued;
    protected final Iter.Single iter = new Iter.Single();


    protected LongValues(boolean multiValued) {
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

    public abstract long getValue(int docId);

    public long getValueMissing(int docId, long missingValue) {
        if (hasValue(docId)) {
            return getValue(docId);
        }
        return missingValue;
    }

    public Iter getIter(int docId) {
        assert !isMultiValued();
        if (hasValue(docId)) {
            return iter.reset(getValue(docId));
        } else {
            return Iter.Empty.INSTANCE;
        }
    }


    public static abstract class Dense extends LongValues {


        protected Dense(boolean multiValued) {
            super(multiValued);
        }

        @Override
        public final boolean hasValue(int docId) {
            return true;
        }

        public final long getValueMissing(int docId, long missingValue) {
            assert hasValue(docId);
            assert !isMultiValued();
            return getValue(docId);
        }

        public final Iter getIter(int docId) {
            assert hasValue(docId);
            assert !isMultiValued();
            return iter.reset(getValue(docId));
        }

    }

    public static abstract class WithOrdinals extends LongValues {

        protected final Docs ordinals;
        private final Iter.Multi iter;

        protected WithOrdinals(Ordinals.Docs ordinals) {
            super(ordinals.isMultiValued());
            this.ordinals = ordinals;
            iter = new Iter.Multi(this);
        }

        public Docs ordinals() {
            return this.ordinals;
        }

        @Override
        public final boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != 0;
        }

        @Override
        public final long getValue(int docId) {
            return getValueByOrd(ordinals.getOrd(docId));
        }

        public abstract long getValueByOrd(long ord);

        @Override
        public final Iter getIter(int docId) {
            return iter.reset(ordinals.getIter(docId));
        }

        @Override
        public final long getValueMissing(int docId, long missingValue) {
            final long ord = ordinals.getOrd(docId);
            if (ord == 0) {
                return missingValue;
            } else {
                return getValueByOrd(ord);
            }
        }

    }

    public static interface Iter {

        boolean hasNext();

        long next();

        public static class Empty implements Iter {

            public static final Empty INSTANCE = new Empty();

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public long next() {
                throw new ElasticSearchIllegalStateException();
            }
        }

        static class Single implements Iter {

            public long value;
            public boolean done;

            public Single reset(long value) {
                this.value = value;
                this.done = false;
                return this;
            }

            @Override
            public boolean hasNext() {
                return !done;
            }

            @Override
            public long next() {
                assert !done;
                done = true;
                return value;
            }
        }

        static class Multi implements Iter {

            private org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs.Iter ordsIter;
            private long ord;
            private WithOrdinals values;

            public Multi(WithOrdinals values) {
                this.values = values;
            }

            public Multi reset(Ordinals.Docs.Iter ordsIter) {
                this.ordsIter = ordsIter;
                this.ord = ordsIter.next();
                return this;
            }

            @Override
            public boolean hasNext() {
                return ord != 0;
            }

            @Override
            public long next() {
                long value = values.getValueByOrd(ord);
                ord = ordsIter.next();
                return value;
            }
        }
    }

    static class Empty extends LongValues {

        public Empty() {
            super(false);
        }

        @Override
        public boolean hasValue(int docId) {
            return false;
        }

        @Override
        public long getValue(int docId) {
            throw new ElasticSearchIllegalStateException("Can't retrieve a value from an empty LongValues");
        }

        @Override
        public Iter getIter(int docId) {
            return Iter.Empty.INSTANCE;
        }

    }

    public static class Filtered extends LongValues {

        protected final LongValues delegate;

        public Filtered(LongValues delegate) {
            super(delegate.isMultiValued());
            this.delegate = delegate;
        }

        public boolean hasValue(int docId) {
            return delegate.hasValue(docId);
        }

        public long getValue(int docId) {
            return delegate.getValue(docId);
        }

        public Iter getIter(int docId) {
            return delegate.getIter(docId);
        }
    }

}
