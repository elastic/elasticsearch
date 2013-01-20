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
import org.elasticsearch.index.fielddata.util.DoubleArrayRef;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.elasticsearch.index.fielddata.util.LongArrayRef;

/**
 */
public interface DoubleValues {

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    boolean isMultiValued();

    /**
     * Is there a value for this doc?
     */
    boolean hasValue(int docId);

    double getValue(int docId);

    double getValueMissing(int docId, double missingValue);

    DoubleArrayRef getValues(int docId);

    Iter getIter(int docId);

    void forEachValueInDoc(int docId, ValueInDocProc proc);

    static interface ValueInDocProc {
        void onValue(int docId, double value);

        void onMissing(int docId);
    }

    static interface Iter {

        boolean hasNext();

        double next();

        static class Empty implements Iter {

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
    }

    public static class LongBased implements DoubleValues {

        private final LongValues values;
        private final DoubleArrayRef arrayScratch = new DoubleArrayRef(new double[1], 1);
        private final ValueIter iter = new ValueIter();
        private final Proc proc = new Proc();

        public LongBased(LongValues values) {
            this.values = values;
        }

        @Override
        public boolean isMultiValued() {
            return values.isMultiValued();
        }

        @Override
        public boolean hasValue(int docId) {
            return values.hasValue(docId);
        }

        @Override
        public double getValue(int docId) {
            return (double) values.getValue(docId);
        }

        @Override
        public double getValueMissing(int docId, double missingValue) {
            if (!values.hasValue(docId)) {
                return missingValue;
            }
            return getValue(docId);
        }

        @Override
        public DoubleArrayRef getValues(int docId) {
            LongArrayRef arrayRef = values.getValues(docId);
            int size = arrayRef.size();
            if (size == 0) {
                return DoubleArrayRef.EMPTY;
            }
            arrayScratch.reset(size);
            for (int i = arrayRef.start; i < arrayRef.end; i++) {
                arrayScratch.values[arrayScratch.end++] = (double) arrayRef.values[i];
            }
            return arrayScratch;
        }

        @Override
        public Iter getIter(int docId) {
            return this.iter.reset(values.getIter(docId));
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            values.forEachValueInDoc(docId, this.proc.reset(proc));
        }

        static class ValueIter implements Iter {

            private LongValues.Iter iter;

            private ValueIter reset(LongValues.Iter iter) {
                this.iter = iter;
                return this;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public double next() {
                return (double) iter.next();
            }
        }

        static class Proc implements LongValues.ValueInDocProc {

            private ValueInDocProc proc;

            private Proc reset(ValueInDocProc proc) {
                this.proc = proc;
                return this;
            }

            @Override
            public void onValue(int docId, long value) {
                this.proc.onValue(docId, (double) value);
            }

            @Override
            public void onMissing(int docId) {
                this.proc.onMissing(docId);
            }
        }

    }
}
