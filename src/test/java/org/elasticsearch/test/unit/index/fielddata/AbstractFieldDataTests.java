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

package org.elasticsearch.test.unit.index.fielddata;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 */
@Test
public abstract class AbstractFieldDataTests {

    protected IndexFieldDataService ifdService;
    protected IndexWriter writer;
    protected AtomicReaderContext readerContext;

    protected abstract FieldDataType getFieldDataType();

    public <IFD extends IndexFieldData> IFD getForField(String fieldName) {
        return ifdService.getForField(fieldName, getFieldDataType());
    }

    @BeforeMethod
    public void setup() throws Exception {
        ifdService = new IndexFieldDataService(new Index("test"));
        writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.VERSION, new StandardAnalyzer(Lucene.VERSION)));
    }

    protected AtomicReaderContext refreshReader() throws Exception {
        if (readerContext != null) {
            readerContext.reader().close();
        }
        AtomicReader reader = new SlowCompositeReaderWrapper(DirectoryReader.open(writer, true));
        readerContext = reader.getContext();
        return readerContext;
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (readerContext != null) {
            readerContext.reader().close();
        }
        writer.close();
        ifdService.clear();
    }

    public static class BytesValuesVerifierProc implements BytesValues.ValueInDocProc {

        private static final BytesRef MISSING = new BytesRef();

        private final int docId;
        private final List<BytesRef> expected = new ArrayList<BytesRef>();

        private int idx;

        BytesValuesVerifierProc(int docId) {
            this.docId = docId;
        }

        public BytesValuesVerifierProc addExpected(String value) {
            expected.add(new BytesRef(value));
            return this;
        }

        public BytesValuesVerifierProc addExpected(BytesRef value) {
            expected.add(value);
            return this;
        }

        public BytesValuesVerifierProc addMissing() {
            expected.add(MISSING);
            return this;
        }

        @Override
        public void onValue(int docId, BytesRef value) {
            assertThat(docId, equalTo(this.docId));
            assertThat(value, equalTo(expected.get(idx++)));
        }

        @Override
        public void onMissing(int docId) {
            assertThat(docId, equalTo(this.docId));
            assertThat(MISSING, sameInstance(expected.get(idx++)));
        }
    }

    public static class HashedBytesValuesVerifierProc implements HashedBytesValues.ValueInDocProc {

        private static final HashedBytesRef MISSING = new HashedBytesRef();

        private final int docId;
        private final List<HashedBytesRef> expected = new ArrayList<HashedBytesRef>();

        private int idx;

        HashedBytesValuesVerifierProc(int docId) {
            this.docId = docId;
        }

        public HashedBytesValuesVerifierProc addExpected(String value) {
            expected.add(new HashedBytesRef(value));
            return this;
        }

        public HashedBytesValuesVerifierProc addExpected(BytesRef value) {
            expected.add(new HashedBytesRef(value));
            return this;
        }

        public HashedBytesValuesVerifierProc addMissing() {
            expected.add(MISSING);
            return this;
        }

        @Override
        public void onValue(int docId, HashedBytesRef value) {
            assertThat(docId, equalTo(this.docId));
            assertThat(value, equalTo(expected.get(idx++)));
        }

        @Override
        public void onMissing(int docId) {
            assertThat(docId, equalTo(this.docId));
            assertThat(MISSING, sameInstance(expected.get(idx++)));
        }
    }

    public static class StringValuesVerifierProc implements StringValues.ValueInDocProc {

        private static final String MISSING = new String();

        private final int docId;
        private final List<String> expected = new ArrayList<String>();

        private int idx;

        StringValuesVerifierProc(int docId) {
            this.docId = docId;
        }

        public StringValuesVerifierProc addExpected(String value) {
            expected.add(value);
            return this;
        }

        public StringValuesVerifierProc addMissing() {
            expected.add(MISSING);
            return this;
        }

        @Override
        public void onValue(int docId, String value) {
            assertThat(docId, equalTo(this.docId));
            assertThat(value, equalTo(expected.get(idx++)));
        }

        @Override
        public void onMissing(int docId) {
            assertThat(docId, equalTo(this.docId));
            assertThat(MISSING, sameInstance(expected.get(idx++)));
        }
    }

    public static class LongValuesVerifierProc implements LongValues.ValueInDocProc {

        private static final Long MISSING = new Long(0);

        private final int docId;
        private final List<Long> expected = new ArrayList<Long>();

        private int idx;

        LongValuesVerifierProc(int docId) {
            this.docId = docId;
        }

        public LongValuesVerifierProc addExpected(long value) {
            expected.add(value);
            return this;
        }

        public LongValuesVerifierProc addMissing() {
            expected.add(MISSING);
            return this;
        }

        @Override
        public void onValue(int docId, long value) {
            assertThat(docId, equalTo(this.docId));
            assertThat(value, equalTo(expected.get(idx++)));
        }

        @Override
        public void onMissing(int docId) {
            assertThat(docId, equalTo(this.docId));
            assertThat(MISSING, sameInstance(expected.get(idx++)));
        }
    }

    public static class DoubleValuesVerifierProc implements DoubleValues.ValueInDocProc {

        private static final Double MISSING = new Double(0);

        private final int docId;
        private final List<Double> expected = new ArrayList<Double>();

        private int idx;

        DoubleValuesVerifierProc(int docId) {
            this.docId = docId;
        }

        public DoubleValuesVerifierProc addExpected(double value) {
            expected.add(value);
            return this;
        }

        public DoubleValuesVerifierProc addMissing() {
            expected.add(MISSING);
            return this;
        }

        @Override
        public void onValue(int docId, double value) {
            assertThat(docId, equalTo(this.docId));
            assertThat(value, equalTo(expected.get(idx++)));
        }

        @Override
        public void onMissing(int docId) {
            assertThat(docId, equalTo(this.docId));
            assertThat(MISSING, sameInstance(expected.get(idx++)));
        }
    }

    public static class ByteValuesVerifierProc implements ByteValues.ValueInDocProc {

        private static final Byte MISSING = new Byte((byte) 0);

        private final int docId;
        private final List<Byte> expected = new ArrayList<Byte>();

        private int idx;

        ByteValuesVerifierProc(int docId) {
            this.docId = docId;
        }

        public ByteValuesVerifierProc addExpected(byte value) {
            expected.add(value);
            return this;
        }

        public ByteValuesVerifierProc addMissing() {
            expected.add(MISSING);
            return this;
        }

        @Override
        public void onValue(int docId, byte value) {
            assertThat(docId, equalTo(this.docId));
            assertThat(value, equalTo(expected.get(idx++)));
        }

        @Override
        public void onMissing(int docId) {
            assertThat(docId, equalTo(this.docId));
            assertThat(MISSING, sameInstance(expected.get(idx++)));
        }
    }

    public static class ShortValuesVerifierProc implements ShortValues.ValueInDocProc {

        private static final Short MISSING = new Short((byte) 0);

        private final int docId;
        private final List<Short> expected = new ArrayList<Short>();

        private int idx;

        ShortValuesVerifierProc(int docId) {
            this.docId = docId;
        }

        public ShortValuesVerifierProc addExpected(short value) {
            expected.add(value);
            return this;
        }

        public ShortValuesVerifierProc addMissing() {
            expected.add(MISSING);
            return this;
        }

        @Override
        public void onValue(int docId, short value) {
            assertThat(docId, equalTo(this.docId));
            assertThat(value, equalTo(expected.get(idx++)));
        }

        @Override
        public void onMissing(int docId) {
            assertThat(docId, equalTo(this.docId));
            assertThat(MISSING, sameInstance(expected.get(idx++)));
        }
    }

    public static class IntValuesVerifierProc implements IntValues.ValueInDocProc {

        private static final Integer MISSING = new Integer(0);

        private final int docId;
        private final List<Integer> expected = new ArrayList<Integer>();

        private int idx;

        IntValuesVerifierProc(int docId) {
            this.docId = docId;
        }

        public IntValuesVerifierProc addExpected(int value) {
            expected.add(value);
            return this;
        }

        public IntValuesVerifierProc addMissing() {
            expected.add(MISSING);
            return this;
        }

        @Override
        public void onValue(int docId, int value) {
            assertThat(docId, equalTo(this.docId));
            assertThat(value, equalTo(expected.get(idx++)));
        }

        @Override
        public void onMissing(int docId) {
            assertThat(docId, equalTo(this.docId));
            assertThat(MISSING, sameInstance(expected.get(idx++)));
        }
    }

    public static class FloatValuesVerifierProc implements FloatValues.ValueInDocProc {

        private static final Float MISSING = new Float(0);

        private final int docId;
        private final List<Float> expected = new ArrayList<Float>();

        private int idx;

        FloatValuesVerifierProc(int docId) {
            this.docId = docId;
        }

        public FloatValuesVerifierProc addExpected(float value) {
            expected.add(value);
            return this;
        }

        public FloatValuesVerifierProc addMissing() {
            expected.add(MISSING);
            return this;
        }

        @Override
        public void onValue(int docId, float value) {
            assertThat(docId, equalTo(this.docId));
            assertThat(value, equalTo(expected.get(idx++)));
        }

        @Override
        public void onMissing(int docId) {
            assertThat(docId, equalTo(this.docId));
            assertThat(MISSING, sameInstance(expected.get(idx++)));
        }
    }
}
