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
import org.elasticsearch.index.fielddata.util.FloatArrayRef;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.elasticsearch.index.fielddata.util.LongArrayRef;
import org.elasticsearch.index.fielddata.util.StringArrayRef;

/**
 */
public interface StringValues {

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    boolean isMultiValued();

    /**
     * Is there a value for this doc?
     */
    boolean hasValue(int docId);

    String getValue(int docId);

    StringArrayRef getValues(int docId);

    Iter getIter(int docId);

    /**
     * Go over all the possible values.
     */
    void forEachValueInDoc(int docId, ValueInDocProc proc);

    public static interface ValueInDocProc {
        void onValue(int docId, String value);

        void onMissing(int docId);
    }


    static interface Iter {

        boolean hasNext();

        String next();

        static class Empty implements Iter {

            public static final Empty INSTANCE = new Empty();

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public String next() {
                throw new ElasticSearchIllegalStateException();
            }
        }

        static class Single implements Iter {

            public String value;
            public boolean done;

            public Single reset(String value) {
                this.value = value;
                this.done = false;
                return this;
            }

            @Override
            public boolean hasNext() {
                return !done;
            }

            @Override
            public String next() {
                assert !done;
                done = true;
                return value;
            }
        }
    }

    public static class IntBased implements StringValues {

        private final IntValues values;

        private final StringArrayRef arrayScratch = new StringArrayRef(new String[1], 1);
        private final ValuesIter valuesIter = new ValuesIter();
        private final Proc proc = new Proc();

        public IntBased(IntValues values) {
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
        public String getValue(int docId) {
            if (!values.hasValue(docId)) {
                return null;
            }
            return Integer.toString(values.getValue(docId));
        }

        @Override
        public StringArrayRef getValues(int docId) {
            IntArrayRef arrayRef = values.getValues(docId);
            int size = arrayRef.size();
            if (size == 0) return StringArrayRef.EMPTY;

            arrayScratch.reset(size);
            for (int i = arrayRef.start; i < arrayRef.end; i++) {
                arrayScratch.values[arrayScratch.end++] = Integer.toString(arrayRef.values[i]);
            }
            return arrayScratch;
        }

        @Override
        public Iter getIter(int docId) {
            return valuesIter.reset(values.getIter(docId));
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            values.forEachValueInDoc(docId, this.proc.reset(proc));
        }

        static class ValuesIter implements Iter {

            private IntValues.Iter iter;

            private ValuesIter reset(IntValues.Iter iter) {
                this.iter = iter;
                return this;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public String next() {
                return Integer.toString(iter.next());
            }
        }

        static class Proc implements IntValues.ValueInDocProc {

            private ValueInDocProc proc;

            private Proc reset(ValueInDocProc proc) {
                this.proc = proc;
                return this;
            }

            @Override
            public void onValue(int docId, int value) {
                proc.onValue(docId, Integer.toString(value));
            }

            @Override
            public void onMissing(int docId) {
                proc.onMissing(docId);
            }
        }
    }

    public static class LongBased implements StringValues {

        private final LongValues values;

        private final StringArrayRef arrayScratch = new StringArrayRef(new String[1], 1);
        private final ValuesIter valuesIter = new ValuesIter();
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
        public String getValue(int docId) {
            if (!values.hasValue(docId)) {
                return null;
            }
            return Long.toString(values.getValue(docId));
        }

        @Override
        public StringArrayRef getValues(int docId) {
            LongArrayRef arrayRef = values.getValues(docId);
            int size = arrayRef.size();
            if (size == 0) return StringArrayRef.EMPTY;

            arrayScratch.reset(size);
            for (int i = arrayRef.start; i < arrayRef.end; i++) {
                arrayScratch.values[arrayScratch.end++] = Long.toString(arrayRef.values[i]);
            }
            return arrayScratch;
        }

        @Override
        public Iter getIter(int docId) {
            return valuesIter.reset(values.getIter(docId));
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            values.forEachValueInDoc(docId, this.proc.reset(proc));
        }

        static class ValuesIter implements Iter {

            private LongValues.Iter iter;

            private ValuesIter reset(LongValues.Iter iter) {
                this.iter = iter;
                return this;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public String next() {
                return Long.toString(iter.next());
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
                proc.onValue(docId, Long.toString(value));
            }

            @Override
            public void onMissing(int docId) {
                proc.onMissing(docId);
            }
        }
    }

    public static class FloatBased implements StringValues {

        private final FloatValues values;

        private final StringArrayRef arrayScratch = new StringArrayRef(new String[1], 1);
        private final ValuesIter valuesIter = new ValuesIter();
        private final Proc proc = new Proc();

        public FloatBased(FloatValues values) {
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
        public String getValue(int docId) {
            if (!values.hasValue(docId)) {
                return null;
            }
            return Float.toString(values.getValue(docId));
        }

        @Override
        public StringArrayRef getValues(int docId) {
            FloatArrayRef arrayRef = values.getValues(docId);
            int size = arrayRef.size();
            if (size == 0) return StringArrayRef.EMPTY;

            arrayScratch.reset(size);
            for (int i = arrayRef.start; i < arrayRef.end; i++) {
                arrayScratch.values[arrayScratch.end++] = Float.toString(arrayRef.values[i]);
            }
            return arrayScratch;
        }

        @Override
        public Iter getIter(int docId) {
            return valuesIter.reset(values.getIter(docId));
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            values.forEachValueInDoc(docId, this.proc.reset(proc));
        }

        static class ValuesIter implements Iter {

            private FloatValues.Iter iter;

            private ValuesIter reset(FloatValues.Iter iter) {
                this.iter = iter;
                return this;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public String next() {
                return Float.toString(iter.next());
            }
        }

        static class Proc implements FloatValues.ValueInDocProc {

            private ValueInDocProc proc;

            private Proc reset(ValueInDocProc proc) {
                this.proc = proc;
                return this;
            }

            @Override
            public void onValue(int docId, float value) {
                proc.onValue(docId, Float.toString(value));
            }

            @Override
            public void onMissing(int docId) {
                proc.onMissing(docId);
            }
        }
    }

}
