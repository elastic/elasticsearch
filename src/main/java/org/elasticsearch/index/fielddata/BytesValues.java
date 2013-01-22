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
import org.elasticsearch.index.fielddata.util.BytesRefArrayRef;
import org.elasticsearch.index.fielddata.util.StringArrayRef;

/**
 */
public interface BytesValues {

    static final BytesValues EMPTY = new Empty();

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    boolean isMultiValued();

    /**
     * Is there a value for this doc?
     */
    boolean hasValue(int docId);

    /**
     * Converts the provided bytes to "safe" ones from a "non" safe call made (if needed).
     */
    BytesRef makeSafe(BytesRef bytes);

    /**
     * Returns a bytes value for a docId. Note, the content of it might be shared across invocation.
     */
    BytesRef getValue(int docId);

    /**
     * Returns the bytes value for the docId, with the provided "ret" which will be filled with the
     * result which will also be returned. If there is no value for this docId, the length will be 0.
     * Note, the bytes are not "safe".
     */
    BytesRef getValueScratch(int docId, BytesRef ret);

    /**
     * Returns an array wrapping all the bytes values for a doc. The content is guaranteed not to be shared.
     */
    BytesRefArrayRef getValues(int docId);

    /**
     * Returns a bytes value iterator for a docId. Note, the content of it might be shared across invocation.
     */
    Iter getIter(int docId);

    /**
     * Go over all the possible values in their BytesRef format for a specific doc.
     */
    void forEachValueInDoc(int docId, ValueInDocProc proc);

    public static interface ValueInDocProc {
        void onValue(int docId, BytesRef value);

        void onMissing(int docId);
    }

    static interface Iter {

        boolean hasNext();

        BytesRef next();

        static class Empty implements Iter {

            public static final Empty INSTANCE = new Empty();

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public BytesRef next() {
                throw new ElasticSearchIllegalStateException();
            }
        }

        static class Single implements Iter {

            public BytesRef value;
            public boolean done;

            public Single reset(BytesRef value) {
                this.value = value;
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
        }
    }

    static class Empty implements BytesValues {
        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean hasValue(int docId) {
            return false;
        }

        @Override
        public BytesRef getValue(int docId) {
            return null;
        }

        @Override
        public BytesRefArrayRef getValues(int docId) {
            return BytesRefArrayRef.EMPTY;
        }

        @Override
        public Iter getIter(int docId) {
            return Iter.Empty.INSTANCE;
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            proc.onMissing(docId);
        }

        @Override
        public BytesRef makeSafe(BytesRef bytes) {
            //todo we can also throw an excepiton here as the only value this method accepts is a scratch value...
            //todo ...extracted from this ByteValues, in our case, there are not values, so this should never be called!?!?
            return BytesRef.deepCopyOf(bytes);
        }

        @Override
        public BytesRef getValueScratch(int docId, BytesRef ret) {
            ret.length = 0;
            return ret;
        }
    }

    public static class StringBased implements BytesValues {

        private final StringValues values;

        protected final BytesRef scratch = new BytesRef();
        private final BytesRefArrayRef arrayScratch = new BytesRefArrayRef(new BytesRef[1], 1);
        private final ValueIter valueIter = new ValueIter();
        private final Proc proc = new Proc();

        public StringBased(StringValues values) {
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
        public BytesRef makeSafe(BytesRef bytes) {
            // we need to make a copy, since we use scratch to provide it
            return BytesRef.deepCopyOf(bytes);
        }

        @Override
        public BytesRef getValue(int docId) {
            String value = values.getValue(docId);
            if (value == null) return null;
            scratch.copyChars(value);
            return scratch;
        }

        @Override
        public BytesRef getValueScratch(int docId, BytesRef ret) {
            String value = values.getValue(docId);
            if (value == null) {
                ret.length = 0;
                return ret;
            }
            ret.copyChars(value);
            return ret;
        }

        @Override
        public BytesRefArrayRef getValues(int docId) {
            StringArrayRef arrayRef = values.getValues(docId);
            int size = arrayRef.size();
            if (size == 0) {
                return BytesRefArrayRef.EMPTY;
            }
            arrayScratch.reset(size);
            for (int i = arrayRef.start; i < arrayRef.end; i++) {
                String value = arrayRef.values[i];
                arrayScratch.values[arrayScratch.end++] = value == null ? null : new BytesRef(value);
            }
            return arrayScratch;
        }

        @Override
        public Iter getIter(int docId) {
            return valueIter.reset(values.getIter(docId));
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            values.forEachValueInDoc(docId, this.proc.reset(proc));
        }

        static class ValueIter implements Iter {

            private final BytesRef scratch = new BytesRef();
            private StringValues.Iter iter;

            public ValueIter reset(StringValues.Iter iter) {
                this.iter = iter;
                return this;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public BytesRef next() {
                scratch.copyChars(iter.next());
                return scratch;
            }
        }

        static class Proc implements StringValues.ValueInDocProc {

            private final BytesRef scratch = new BytesRef();
            private BytesValues.ValueInDocProc proc;

            public Proc reset(BytesValues.ValueInDocProc proc) {
                this.proc = proc;
                return this;
            }

            @Override
            public void onValue(int docId, String value) {
                scratch.copyChars(value);
                proc.onValue(docId, scratch);
            }

            @Override
            public void onMissing(int docId) {
                proc.onMissing(docId);
            }
        }
    }
}
