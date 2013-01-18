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
import org.elasticsearch.index.fielddata.util.FloatArrayRef;

/**
 */
public interface FloatValues {

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    boolean isMultiValued();

    /**
     * Is there a value for this doc?
     */
    boolean hasValue(int docId);

    float getValue(int docId);

    float getValueMissing(int docId, float missingValue);

    FloatArrayRef getValues(int docId);

    Iter getIter(int docId);

    void forEachValueInDoc(int docId, ValueInDocProc proc);

    static interface ValueInDocProc {
        void onValue(int docId, float value);

        void onMissing(int docId);
    }

    static interface Iter {

        boolean hasNext();

        float next();

        static class Empty implements Iter {

            public static final Empty INSTANCE = new Empty();

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public float next() {
                throw new ElasticSearchIllegalStateException();
            }
        }

        static class Single implements Iter {

            public float value;
            public boolean done;

            public Single reset(float value) {
                this.value = value;
                this.done = false;
                return this;
            }

            @Override
            public boolean hasNext() {
                return !done;
            }

            @Override
            public float next() {
                assert !done;
                done = true;
                return value;
            }
        }
    }

    public static class DoubleBased implements FloatValues {

        private final DoubleValues values;

        private final FloatArrayRef arrayScratch = new FloatArrayRef(new float[1], 1);
        private final ValueIter iter = new ValueIter();
        private final Proc proc = new Proc();

        public DoubleBased(DoubleValues values) {
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
        public float getValue(int docId) {
            return (float) values.getValue(docId);
        }

        @Override
        public float getValueMissing(int docId, float missingValue) {
            return (float) values.getValueMissing(docId, missingValue);
        }

        @Override
        public FloatArrayRef getValues(int docId) {
            DoubleArrayRef arrayRef = values.getValues(docId);
            int size = arrayRef.size();
            if (size == 0) {
                return FloatArrayRef.EMPTY;
            }
            arrayScratch.reset(size);
            for (int i = arrayRef.start; i < arrayRef.end; i++) {
                arrayScratch.values[arrayScratch.end++] = (float) arrayRef.values[i];
            }
            return arrayScratch;
        }

        @Override
        public Iter getIter(int docId) {
            return iter.reset(values.getIter(docId));
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            values.forEachValueInDoc(docId, this.proc.reset(proc));
        }

        static class ValueIter implements Iter {

            private DoubleValues.Iter iter;

            public ValueIter reset(DoubleValues.Iter iter) {
                this.iter = iter;
                return this;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public float next() {
                return (float) iter.next();
            }
        }

        static class Proc implements DoubleValues.ValueInDocProc {

            private ValueInDocProc proc;

            public Proc reset(ValueInDocProc proc) {
                this.proc = proc;
                return this;
            }

            @Override
            public void onValue(int docId, double value) {
                proc.onValue(docId, (float) value);
            }

            @Override
            public void onMissing(int docId) {
                proc.onMissing(docId);
            }
        }
    }
}
