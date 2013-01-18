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
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.elasticsearch.index.fielddata.util.LongArrayRef;

/**
 */
public interface IntValues {

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    boolean isMultiValued();

    /**
     * Is there a value for this doc?
     */
    boolean hasValue(int docId);

    int getValue(int docId);

    int getValueMissing(int docId, int missingValue);

    IntArrayRef getValues(int docId);

    Iter getIter(int docId);

    void forEachValueInDoc(int docId, ValueInDocProc proc);

    static interface ValueInDocProc {
        void onValue(int docId, int value);

        void onMissing(int docId);
    }

    static interface Iter {

        boolean hasNext();

        int next();

        static class Empty implements Iter {

            public static final Empty INSTANCE = new Empty();

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public int next() {
                throw new ElasticSearchIllegalStateException();
            }
        }

        static class Single implements Iter {

            public int value;
            public boolean done;

            public Single reset(int value) {
                this.value = value;
                this.done = false;
                return this;
            }

            @Override
            public boolean hasNext() {
                return !done;
            }

            @Override
            public int next() {
                assert !done;
                done = true;
                return value;
            }
        }
    }

    public static class LongBased implements IntValues {

        private final LongValues values;

        private final IntArrayRef arrayScratch = new IntArrayRef(new int[1], 1);
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
        public int getValue(int docId) {
            return (int) values.getValue(docId);
        }

        @Override
        public int getValueMissing(int docId, int missingValue) {
            return (int) values.getValueMissing(docId, missingValue);
        }

        @Override
        public IntArrayRef getValues(int docId) {
            LongArrayRef arrayRef = values.getValues(docId);
            int size = arrayRef.size();
            if (size == 0) {
                return IntArrayRef.EMPTY;
            }
            arrayScratch.reset(size);
            for (int i = arrayRef.start; i < arrayRef.end; i++) {
                arrayScratch.values[arrayScratch.end++] = (int) arrayRef.values[i];
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

            private LongValues.Iter iter;

            public ValueIter reset(LongValues.Iter iter) {
                this.iter = iter;
                return this;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public int next() {
                return (int) iter.next();
            }
        }

        static class Proc implements LongValues.ValueInDocProc {

            private ValueInDocProc proc;

            public Proc reset(ValueInDocProc proc) {
                this.proc = proc;
                return this;
            }

            @Override
            public void onValue(int docId, long value) {
                proc.onValue(docId, (int) value);
            }

            @Override
            public void onMissing(int docId) {
                proc.onMissing(docId);
            }
        }
    }
}
