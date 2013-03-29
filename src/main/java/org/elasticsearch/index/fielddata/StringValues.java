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
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.util.DoubleArrayRef;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.elasticsearch.index.fielddata.util.LongArrayRef;
import org.elasticsearch.index.fielddata.util.StringArrayRef;

/**
 */
public interface StringValues {

    static final StringValues EMPTY = new Empty();

    /**
     * Is one of the documents in this field data values is multi valued?
     */
    boolean isMultiValued();

    /**
     * Is there a value for this doc?
     */
    boolean hasValue(int docId);

    String getValue(int docId);

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

    static class Empty implements StringValues {
        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean hasValue(int docId) {
            return false;
        }

        @Override
        public String getValue(int docId) {
            return null;
        }

        @Override
        public Iter getIter(int docId) {
            return Iter.Empty.INSTANCE;
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            proc.onMissing(docId);
        }
    }
    
    
    static class DoubleBased implements StringValues {
        private final DoubleValues values;

        private final StringArrayRef arrayScratch = new StringArrayRef(new String[1], 1);
        private final ValuesIter valuesIter = new ValuesIter();

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
        public String getValue(int docId) {
            if (!values.hasValue(docId)) {
                return null;
            }
            return Double.toString(values.getValue(docId));
        }

        @Override
        public Iter getIter(int docId) {
            return valuesIter.reset(values.getIter(docId));
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            if (values.hasValue(docId)) {
                final DoubleValues.Iter doubleIter = values.getIter(docId);
                while(doubleIter.hasNext()) {
                    proc.onValue(docId, Double.toString(doubleIter.next()));
                }
            } else {
                proc.onMissing(docId);
            }
        }

        static class ValuesIter implements Iter {

            private DoubleValues.Iter iter;

            private ValuesIter reset(DoubleValues.Iter iter) {
                this.iter = iter;
                return this;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public String next() {
                return Double.toString(iter.next());
            }
        }

    }
   
    public static class LongBased implements StringValues {

        private final LongValues values;
        private final ValuesIter valuesIter = new ValuesIter();

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
        public Iter getIter(int docId) {
            return valuesIter.reset(values.getIter(docId));
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            if (values.hasValue(docId)) {
                final LongValues.Iter longIter = values.getIter(docId);
                while(longIter.hasNext()) {
                    proc.onValue(docId, Long.toString(longIter.next()));
                }
            } else {
                proc.onMissing(docId);
            }
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

        
    }

    public interface WithOrdinals extends StringValues {

        Ordinals.Docs ordinals();

        String getValueByOrd(int ord);

        public static class Empty extends StringValues.Empty implements WithOrdinals {

            private final Ordinals ordinals;

            public Empty(Ordinals ordinals) {
                this.ordinals = ordinals;
            }

            @Override
            public Ordinals.Docs ordinals() {
                return ordinals.ordinals();
            }

            @Override
            public String getValueByOrd(int ord) {
                return null;
            }
        }
    }
    
    public static class BytesValuesWrapper implements StringValues.WithOrdinals {
        private org.elasticsearch.index.fielddata.BytesValues.WithOrdinals delegate;
        private final CharsRef spare = new CharsRef();
        protected final Docs ordinals;
        protected final StringArrayRef arrayScratch;
        private final OrdinalIter iter = new OrdinalIter(this);
        
        BytesValuesWrapper(BytesValues.WithOrdinals delegate) {
            arrayScratch = new StringArrayRef(new String[delegate.isMultiValued() ? 10 : 1], delegate.isMultiValued() ? 0 : 1);
            this.delegate = delegate;
            this.ordinals = delegate.ordinals();
        }

        public static StringValues.WithOrdinals wrap(BytesValues.WithOrdinals values) {
            if (values.isMultiValued()) {
                return new MultiBytesValuesWrapper(values);
            } else {
                return new BytesValuesWrapper(values);
            }
        }
        @Override
        public String getValue(int docId) {
            final BytesRef value = delegate.getValue(docId);
            if (value != null) {
                UnicodeUtil.UTF8toUTF16(value, spare);
                return spare.toString();
            }
            return null;
        }
        
        @Override
        public Iter getIter(int docId) {
            return iter.reset(this.ordinals.getIter(docId));
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            assert !isMultiValued();
            int ord = ordinals.getOrd(docId);
            if (ord == 0) {
                proc.onMissing(docId);
            } else {
                proc.onValue(docId, getValueByOrd(ord));
            }
        }

        @Override
        public Docs ordinals() {
            return delegate.ordinals;
        }

        @Override
        public String getValueByOrd(int ord) {
            final BytesRef value = delegate.getValueByOrd(ord);
            if (value != null) {
                UnicodeUtil.UTF8toUTF16(value, spare);
                return spare.toString();
            }
            return null;
        }


        @Override
        public boolean isMultiValued() {
           return delegate.isMultiValued();
        }


        @Override
        public boolean hasValue(int docId) {
           return delegate.hasValue(docId);
        }
        
    }
    
    static final class MultiBytesValuesWrapper extends BytesValuesWrapper {
        MultiBytesValuesWrapper(org.elasticsearch.index.fielddata.BytesValues.WithOrdinals delegate) {
            super(delegate);
        }

        @Override
        public void forEachValueInDoc(int docId, ValueInDocProc proc) {
            assert isMultiValued();

            Ordinals.Docs.Iter iter = ordinals.getIter(docId);
            int ord = iter.next();
            if (ord == 0) {
                proc.onMissing(docId);
            } else {
                do {
                    proc.onValue(docId, getValueByOrd(ord));
                } while ((ord = iter.next()) != 0);
            }
        }
    }
    
    static final class OrdinalIter implements StringValues.Iter {

        private Ordinals.Docs.Iter ordsIter;
        private int ord;
        private final StringValues.WithOrdinals values;

        OrdinalIter(StringValues.WithOrdinals values) {
            this.values = values;
        }

        public OrdinalIter reset(Ordinals.Docs.Iter ordsIter) {
            this.ordsIter = ordsIter;
            this.ord = ordsIter.next();
            return this;
        }

        @Override
        public boolean hasNext() {
            return ord != 0;
        }

        @Override
        public String next() {
            final String valueByOrd = values.getValueByOrd(ord);
            ord = ordsIter.next();
            return valueByOrd;
        }
    }
}
