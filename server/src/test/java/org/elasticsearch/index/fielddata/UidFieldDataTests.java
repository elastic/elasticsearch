/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;

public class UidFieldDataTests extends ESTestCase {

    private static class DummySortedDocValues extends SortedDocValues {

        private int doc = -1;

        @Override
        public int ordValue() throws IOException {
            return doc;
        }

        @Override
        public BytesRef lookupOrd(int ord) throws IOException {
            return new BytesRef(String.format(Locale.ENGLISH, "%03d", ord));
        }

        @Override
        public int getValueCount() {
            return 100;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            doc = target;
            return true;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= getValueCount()) {
                return doc = NO_MORE_DOCS;
            } else {
                return doc = target;
            }
        }

        @Override
        public long cost() {
            return getValueCount();
        }

    }

    private static class DummyAtomicOrdinalsFieldData implements AtomicOrdinalsFieldData {

        @Override
        public ScriptDocValues<?> getScriptValues() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            return FieldData.toString(getOrdinalsValues());
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public SortedSetDocValues getOrdinalsValues() {
            return DocValues.singleton(new DummySortedDocValues());
        }

    }

    public void testSortedSetValues() throws Exception {
        AtomicFieldData fd = new UidIndexFieldData.UidAtomicFieldData(new BytesRef("type#"), new DummyAtomicOrdinalsFieldData());
        SortedBinaryDocValues dv = fd.getBytesValues();
        assertTrue(dv.advanceExact(30));
        assertEquals(1, dv.docValueCount());
        assertEquals(new BytesRef("type#030"), dv.nextValue());
    }

    public void testScriptValues() throws IOException {
        AtomicFieldData fd = new UidIndexFieldData.UidAtomicFieldData(new BytesRef("type#"), new DummyAtomicOrdinalsFieldData());
        ScriptDocValues<?> values = fd.getScriptValues();
        values.setNextDocId(30);
        assertEquals(Collections.singletonList("type#030"), values);
    }

}
