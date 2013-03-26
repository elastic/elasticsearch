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
import org.elasticsearch.index.fielddata.util.BytesRefArrayRef;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.elasticsearch.index.fielddata.util.StringArrayRef;

/**
 */
public abstract class  BytesValues {

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
     * Converts the provided bytes to "safe" ones from a "non" safe call made (if needed).
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
     * Returns an array wrapping all the bytes values for a doc. The content is guaranteed not to be shared.
     */
    public abstract BytesRefArrayRef getValues(int docId);

    /**
     * Returns a bytes value iterator for a docId. Note, the content of it might be shared across invocation.
     */
    public abstract Iter getIter(int docId);
    
    

    /**
     * Go over all the possible values in their BytesRef format for a specific doc.
     */
    public abstract void forEachValueInDoc(int docId, ValueInDocProc proc);
    public static interface ValueInDocProc {
        void onValue(int docId, BytesRef value);

        void onMissing(int docId);
    }

    public static interface Iter {

        boolean hasNext();

        BytesRef next();

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
        }

        public final static class Single implements Iter {

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
        
        static final class Multi implements Iter {

            private int ord;
            private BytesValues.WithOrdinals withOrds;
            private Ordinals.Docs.Iter ordsIter;
            private final BytesRef scratch = new BytesRef();
            public Multi(WithOrdinals withOrds) {
                this.withOrds = withOrds;
                assert withOrds.isMultiValued();
                
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
            public BytesRef next() {
                withOrds.getValueScratchByOrd(ord, scratch);
                ord = ordsIter.next();
                return scratch;
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
        public BytesRef getValueScratch(int docId, BytesRef ret) {
            ret.length = 0;
            return ret;
        }
    }

    public static class StringBased extends BytesValues {

        
        private final StringValues values;

        private final BytesRefArrayRef arrayScratch = new BytesRefArrayRef(new BytesRef[1], 1);
        private final ValueIter valueIter = new ValueIter();
        private final Proc proc = new Proc();

        public StringBased(StringValues values) {
            super(values.isMultiValued());
            this.values = values;
        }

        @Override
        public boolean hasValue(int docId) {
            return values.hasValue(docId);
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

        public  static class ValueIter implements Iter {

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

        public static class Proc implements StringValues.ValueInDocProc {

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

    /**
     * Bytes values that are based on ordinals.
     */
    public static abstract class WithOrdinals extends BytesValues {
        
        protected final Docs ordinals;
        protected final BytesRefArrayRef arrayScratch = new BytesRefArrayRef(new BytesRef[10], 0);

        protected WithOrdinals(Ordinals.Docs ordinals) {
            super(ordinals.isMultiValued());
            this.ordinals = ordinals;
        }

        public Ordinals.Docs ordinals() {
            return ordinals;
        }

        public BytesRef getValueByOrd(int ord) {
            return getValueScratchByOrd(ord, scratch);
        }

        @Override
        public boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != 0;
        }
        
        @Override
        public BytesRefArrayRef getValues(int docId) {
            assert !isMultiValued();
            int ord = ordinals.getOrd(docId);
            if (ord == 0) return BytesRefArrayRef.EMPTY;
            arrayScratch.values[0] = getSafeValueByOrd(ord);
            arrayScratch.end = 1;
            arrayScratch.start = 0;
            return arrayScratch;
        }
        
        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            assert !isMultiValued();
            int ord = ordinals.getOrd(docId);
            if (ord == 0) {
                proc.onMissing(docId);
            } else {
                proc.onValue(docId, getValue(docId));
            }
        }
        
        protected BytesRefArrayRef getValuesMulti(int docId) {
            assert isMultiValued();
            IntArrayRef ords = ordinals.getOrds(docId);
            int size = ords.size();
            if (size == 0) {
                return BytesRefArrayRef.EMPTY;
            }
            arrayScratch.reset(size);
            for (int i = ords.start; i < ords.end; i++) {
                arrayScratch.values[arrayScratch.end++] = getValueScratchByOrd(ords.values[i], new BytesRef());
            }
            return arrayScratch;
        }

        protected void forEachValueInDocMulti(int docId, ValueInDocProc proc) {
            assert isMultiValued();
            Ordinals.Docs.Iter iter = ordinals.getIter(docId);
            int ord = iter.next();
            if (ord == 0) {
                proc.onMissing(docId);
                return;
            }
            do {
                getValueScratchByOrd(ord, scratch);
                proc.onValue(docId, scratch);
            } while ((ord = iter.next()) != 0);
        }

        @Override
        public BytesRef getValue(int docId) {
            int ord = ordinals.getOrd(docId);
            if (ord == 0) return null;
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
        public abstract BytesRef getValueScratchByOrd(int ord, BytesRef ret);

        public static class Empty extends WithOrdinals {

            public Empty(Ordinals.Docs ordinals) {
                super(ordinals);
            }

            @Override
            public BytesRef getValueByOrd(int ord) {
                return null;
            }

            @Override
            public BytesRef getValueScratchByOrd(int ord, BytesRef ret) {
                ret.length = 0;
                return ret;
            }

            @Override
            public BytesRef getSafeValueByOrd(int ord) {
                return null;
            }

            @Override
            public boolean hasValue(int docId) {
                return false;
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
            public BytesRef getValueScratch(int docId, BytesRef ret) {
                ret.length = 0;
                return ret;
            }
        }
    }
}
