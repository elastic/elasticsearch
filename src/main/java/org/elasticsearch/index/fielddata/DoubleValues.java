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
public abstract class DoubleValues {

    public static final DoubleValues EMPTY = new Empty();
    private final boolean multiValued;
    protected final Iter.Single iter = new Iter.Single();


    protected DoubleValues(boolean multiValued) {
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

    public abstract double getValue(int docId);

    public double getValueMissing(int docId, double missingValue) {
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


    public static abstract class Dense extends DoubleValues {


        protected Dense(boolean multiValued) {
            super(multiValued);
        }

        @Override
        public final boolean hasValue(int docId) {
            return true;
        }

        public final double getValueMissing(int docId, double missingValue) {
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

    public static abstract class WithOrdinals extends DoubleValues {

        protected final Docs ordinals;
        private final Iter.Multi iter;

        protected WithOrdinals(Ordinals.Docs ordinals) {
            super(ordinals.isMultiValued());
            this.ordinals = ordinals;
            iter = new Iter.Multi(this);
        }

        public Docs ordinals() {
            return ordinals;
        }

        @Override
        public final boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != 0;
        }

        @Override
        public final double getValue(int docId) {
            return getValueByOrd(ordinals.getOrd(docId));
        }

        @Override
        public final double getValueMissing(int docId, double missingValue) {
            final long ord = ordinals.getOrd(docId);
            if (ord == 0) {
                return missingValue;
            } else {
                return getValueByOrd(ord);
            }
        }

        public abstract double getValueByOrd(long ord);

        @Override
        public final Iter getIter(int docId) {
            return iter.reset(ordinals.getIter(docId));
        }

    }

    public static interface Iter {

        boolean hasNext();

        double next();

        public static class Empty implements Iter {

            public static final Empty INSTANCE = new Empty();

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public double next() {
                throw new ElasticSearchIllegalStateException();
            }
        }

        static class Single implements Iter {

            public double value;
            public boolean done;

            public Single reset(double value) {
                this.value = value;
                this.done = false;
                return this;
            }

            @Override
            public boolean hasNext() {
                return !done;
            }

            @Override
            public double next() {
                assert !done;
                done = true;
                return value;
            }
        }

        static class Multi implements Iter {

            private Ordinals.Docs.Iter ordsIter;
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
            public double next() {
                double value = values.getValueByOrd(ord);
                ord = ordsIter.next();
                return value;
            }
        }
    }

    static class Empty extends DoubleValues {

        public Empty() {
            super(false);
        }

        @Override
        public boolean hasValue(int docId) {
            return false;
        }

        @Override
        public double getValue(int docId) {
            throw new ElasticSearchIllegalStateException("Can't retrieve a value from an empty DoubleValues");
        }

        @Override
        public Iter getIter(int docId) {
            return Iter.Empty.INSTANCE;
        }

    }

    public static class Filtered extends DoubleValues {

        protected final DoubleValues delegate;

        public Filtered(DoubleValues delegate) {
            super(delegate.isMultiValued());
            this.delegate = delegate;
        }

        public boolean hasValue(int docId) {
            return delegate.hasValue(docId);
        }

        public double getValue(int docId) {
            return delegate.getValue(docId);
        }

        public Iter getIter(int docId) {
            return delegate.getIter(docId);
        }
    }

}
