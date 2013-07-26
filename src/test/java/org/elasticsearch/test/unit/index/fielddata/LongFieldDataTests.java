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

import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.TDoubleSet;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TDoubleHashSet;
import gnu.trove.set.hash.TLongHashSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.plain.PackedArrayAtomicFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for all integer types (byte, short, int, long).
 */
public class LongFieldDataTests extends NumericFieldDataTests {

    @Override
    protected FieldDataType getFieldDataType() {
        // we don't want to optimize the type so it will always be a long...
        return new FieldDataType("long", ImmutableSettings.builder());
    }

    protected void add2SingleValuedDocumentsAndDeleteOneOfThem() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new LongField("value", 2, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        d.add(new LongField("value", 4, Field.Store.NO));
        writer.addDocument(d);

        writer.commit();

        writer.deleteDocuments(new Term("_id", "1"));
    }

    @Test
    public void testOptimizeTypeLong() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new LongField("value", Integer.MAX_VALUE + 1l, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        d.add(new LongField("value", Integer.MIN_VALUE - 1l, Field.Store.NO));
        writer.addDocument(d);

        IndexNumericFieldData indexFieldData = ifdService.getForField(new FieldMapper.Names("value"), new FieldDataType("long"));
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());
        assertThat(fieldData, instanceOf(PackedArrayAtomicFieldData.class));
        assertThat(fieldData.getLongValues().getValue(0), equalTo((long) Integer.MAX_VALUE + 1l));
        assertThat(fieldData.getLongValues().getValue(1), equalTo((long) Integer.MIN_VALUE - 1l));
    }

    @Test
    public void testDateScripts() throws Exception {
        fillSingleValueAllSet();
        IndexNumericFieldData indexFieldData = getForField("value");
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());

        ScriptDocValues.Longs scriptValues = (ScriptDocValues.Longs) fieldData.getScriptValues();
        scriptValues.setNextDocId(0);
        assertThat(scriptValues.getValue(), equalTo(2l));
        assertThat(scriptValues.getDate().getMillis(), equalTo(2l));
        assertThat(scriptValues.getDate().getZone(), equalTo(DateTimeZone.UTC));
    }

    @Override
    protected void fillSingleValueAllSet() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new LongField("value", 2, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        d.add(new LongField("value", 1, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(new LongField("value", 3, Field.Store.NO));
        writer.addDocument(d);
    }

    @Override
    protected void fillSingleValueWithMissing() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new LongField("value", 2, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        //d.add(new StringField("value", one(), Field.Store.NO)); // MISSING....
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(new LongField("value", 3, Field.Store.NO));
        writer.addDocument(d);
    }

    @Override
    protected void fillMultiValueAllSet() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new LongField("value", 2, Field.Store.NO));
        d.add(new LongField("value", 4, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        d.add(new LongField("value", 1, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(new LongField("value", 3, Field.Store.NO));
        writer.addDocument(d);
    }

    @Override
    protected void fillMultiValueWithMissing() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new LongField("value", 2, Field.Store.NO));
        d.add(new LongField("value", 4, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        //d.add(new StringField("value", one(), Field.Store.NO)); // MISSING
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(new LongField("value", 3, Field.Store.NO));
        writer.addDocument(d);
    }

    protected void fillExtendedMvSet() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new LongField("value", 2, Field.Store.NO));
        d.add(new LongField("value", 4, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(new LongField("value", 3, Field.Store.NO));
        writer.addDocument(d);
        writer.commit();

        d = new Document();
        d.add(new StringField("_id", "4", Field.Store.NO));
        d.add(new LongField("value", 4, Field.Store.NO));
        d.add(new LongField("value", 5, Field.Store.NO));
        d.add(new LongField("value", 6, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "5", Field.Store.NO));
        d.add(new LongField("value", 6, Field.Store.NO));
        d.add(new LongField("value", 7, Field.Store.NO));
        d.add(new LongField("value", 8, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "6", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "7", Field.Store.NO));
        d.add(new LongField("value", 8, Field.Store.NO));
        d.add(new LongField("value", 9, Field.Store.NO));
        d.add(new LongField("value", 10, Field.Store.NO));
        writer.addDocument(d);
        writer.commit();

        d = new Document();
        d.add(new StringField("_id", "8", Field.Store.NO));
        d.add(new LongField("value", -8, Field.Store.NO));
        d.add(new LongField("value", -9, Field.Store.NO));
        d.add(new LongField("value", -10, Field.Store.NO));
        writer.addDocument(d);
    }

    private static final int SECONDS_PER_YEAR = 60 * 60 * 24 * 365;

    // TODO: use random() when migrating to Junit
    public static enum Data {
        SINGLE_VALUED_DENSE_ENUM {
            public int numValues(Random r) {
                return 1;
            }
            @Override
            public long nextValue(Random r) {
                return 1 + r.nextInt(16);
            }
        },
        SINGLE_VALUED_DENSE_DATE {
            public int numValues(Random r) {
                return 1;
            }
            @Override
            public long nextValue(Random r) {
                // somewhere in-between 2010 and 2012
                return 1000L * (40L * SECONDS_PER_YEAR + r.nextInt(2 * SECONDS_PER_YEAR));
            }
        },
        MULTI_VALUED_DATE {
            public int numValues(Random r) {
                return r.nextInt(3);
            }
            @Override
            public long nextValue(Random r) {
                // somewhere in-between 2010 and 2012
                return 1000L * (40L * SECONDS_PER_YEAR + r.nextInt(2 * SECONDS_PER_YEAR));
            }
        },
        MULTI_VALUED_ENUM {
            public int numValues(Random r) {
                return r.nextInt(3);
            }
            @Override
            public long nextValue(Random r) {
                return 3 + r.nextInt(8);
            }
        },
        SINGLE_VALUED_SPARSE_RANDOM {
            public int numValues(Random r) {
                return r.nextFloat() < 0.1f ? 1 : 0;
            }
            @Override
            public long nextValue(Random r) {
                return r.nextLong();
            }
        },
        MULTI_VALUED_SPARSE_RANDOM {
            public int numValues(Random r) {
                return r.nextFloat() < 0.1f ? 1 + r.nextInt(5) : 0;
            }
            @Override
            public long nextValue(Random r) {
                return r.nextLong();
            }
        },
        MULTI_VALUED_DENSE_RANDOM {
            public int numValues(Random r) {
                return 1 + r.nextInt(3);
            }
            @Override
            public long nextValue(Random r) {
                return r.nextLong();
            }
        };
        public abstract int numValues(Random r);
        public abstract long nextValue(Random r);
    }

    private void test(List<TLongSet> values) throws Exception {
        StringField id = new StringField("_id", "", Field.Store.NO);

        for (int i = 0; i < values.size(); ++i) {
            Document doc = new Document();
            id.setStringValue("" + i);
            doc.add(id);
            final TLongSet v = values.get(i);
            for (TLongIterator it = v.iterator(); it.hasNext(); ) {
                LongField value = new LongField("value", it.next(), Field.Store.NO);
                doc.add(value);
            }
            writer.addDocument(doc);
        }
        writer.forceMerge(1);

        final IndexNumericFieldData indexFieldData = getForField("value");
        final AtomicNumericFieldData atomicFieldData = indexFieldData.load(refreshReader());
        final LongValues data = atomicFieldData.getLongValues();
        final DoubleValues doubleData = atomicFieldData.getDoubleValues();
        final TLongSet set = new TLongHashSet();
        final TDoubleSet doubleSet = new TDoubleHashSet();
        for (int i = 0; i < values.size(); ++i) {
            final TLongSet v = values.get(i);

            assertThat(data.hasValue(i), equalTo(!v.isEmpty()));
            assertThat(doubleData.hasValue(i), equalTo(!v.isEmpty()));

            if (v.isEmpty()) {
                assertThat(data.getValue(i), equalTo(0L));
                assertThat(doubleData.getValue(i), equalTo(0d));
            }

            set.clear();
            for (LongValues.Iter iter = data.getIter(i); iter.hasNext(); ) {
                set.add(iter.next());
            }
            assertThat(set, equalTo(v));

            final TDoubleSet doubleV = new TDoubleHashSet();
            for (TLongIterator it = v.iterator(); it.hasNext(); ) {
                doubleV.add((double) it.next());
            }
            doubleSet.clear();
            for (DoubleValues.Iter iter = doubleData.getIter(i); iter.hasNext(); ) {
                doubleSet.add(iter.next());
            }
            assertThat(doubleSet, equalTo(doubleV));
        }
    }

    private void test(Data data) throws Exception {
        Random r = getRandom();
        final int numDocs = 1000 + r.nextInt(19000);
        final List<TLongSet> values = new ArrayList<TLongSet>(numDocs);
        for (int i = 0; i < numDocs; ++i) {
            final int numValues = data.numValues(r);
            final TLongSet vals = new TLongHashSet(numValues);
            for (int j = 0; j < numValues; ++j) {
                vals.add(data.nextValue(r));
            }
            values.add(vals);
        }
        test(values);
    }

    public void testSingleValuedDenseEnum() throws Exception {
        test(Data.SINGLE_VALUED_DENSE_ENUM);
    }

    public void testSingleValuedDenseDate() throws Exception {
        test(Data.SINGLE_VALUED_DENSE_DATE);
    }

    public void testSingleValuedSparseRandom() throws Exception {
        test(Data.SINGLE_VALUED_SPARSE_RANDOM);
    }

    public void testMultiValuedDate() throws Exception {
        test(Data.MULTI_VALUED_DATE);
    }

    public void testMultiValuedEnum() throws Exception {
        test(Data.MULTI_VALUED_ENUM);
    }

    public void testMultiValuedSparseRandom() throws Exception {
        test(Data.MULTI_VALUED_SPARSE_RANDOM);
    }

    public void testMultiValuedDenseRandom() throws Exception {
        test(Data.MULTI_VALUED_DENSE_RANDOM);
    }

}
