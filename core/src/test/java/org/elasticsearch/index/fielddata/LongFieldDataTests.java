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

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests for all integer types (byte, short, int, long).
 */
public class LongFieldDataTests extends AbstractNumericFieldDataTestCase {

    @Override
    protected FieldDataType getFieldDataType() {
        // we don't want to optimize the type so it will always be a long...
        return new FieldDataType("long", getFieldDataSettings());
    }

    @Override
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

        IndexNumericFieldData indexFieldData = getForField("value");
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());
        assertThat(getFirst(fieldData.getLongValues(), 0), equalTo((long) Integer.MAX_VALUE + 1l));
        assertThat(getFirst(fieldData.getLongValues(), 1), equalTo((long) Integer.MIN_VALUE - 1l));
    }

    private static long getFirst(SortedNumericDocValues values, int docId) {
        values.setDocument(docId);
        final int numValues = values.count();
        assertThat(numValues, is(1));
        return values.valueAt(0);
    }

    private static double getFirst(SortedNumericDoubleValues values, int docId) {
        values.setDocument(docId);
        final int numValues = values.count();
        assertThat(numValues, is(1));
        return values.valueAt(0);
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

    @Override
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
            @Override
            public int numValues(Random r) {
                return 1;
            }

            @Override
            public long nextValue(Random r) {
                return 1 + r.nextInt(16);
            }
        },
        SINGLE_VALUED_DENSE_DATE {
            @Override
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
            @Override
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
            @Override
            public int numValues(Random r) {
                return r.nextInt(3);
            }

            @Override
            public long nextValue(Random r) {
                return 3 + r.nextInt(8);
            }
        },
        SINGLE_VALUED_SPARSE_RANDOM {
            @Override
            public int numValues(Random r) {
                return r.nextFloat() < 0.01 ? 1 : 0;
            }

            @Override
            public long nextValue(Random r) {
                return r.nextLong();
            }
        },
        MULTI_VALUED_SPARSE_RANDOM {
            @Override
            public int numValues(Random r) {
                return r.nextFloat() < 0.01f ? 1 + r.nextInt(5) : 0;
            }

            @Override
            public long nextValue(Random r) {
                return r.nextLong();
            }
        },
        MULTI_VALUED_DENSE_RANDOM {
            @Override
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

    private void test(List<LongHashSet> values) throws Exception {
        StringField id = new StringField("_id", "", Field.Store.NO);

        for (int i = 0; i < values.size(); ++i) {
            Document doc = new Document();
            id.setStringValue("" + i);
            doc.add(id);
            final LongHashSet v = values.get(i);
            for (LongCursor c : v) {
                LongField value = new LongField("value", c.value, Field.Store.NO);
                doc.add(value);
            }
            writer.addDocument(doc);
        }
        writer.forceMerge(1, true);

        final IndexNumericFieldData indexFieldData = getForField("value");
        final AtomicNumericFieldData atomicFieldData = indexFieldData.load(refreshReader());
        final SortedNumericDocValues data = atomicFieldData.getLongValues();
        final SortedNumericDoubleValues doubleData = atomicFieldData.getDoubleValues();
        final LongHashSet set = new LongHashSet();
        final LongHashSet doubleSet = new LongHashSet();
        for (int i = 0; i < values.size(); ++i) {
            final LongHashSet v = values.get(i);

            data.setDocument(i);
            assertThat(data.count() > 0, equalTo(!v.isEmpty()));
            doubleData.setDocument(i);
            assertThat(doubleData.count() > 0, equalTo(!v.isEmpty()));

            set.clear();
            data.setDocument(i);
            int numValues = data.count();
            for (int j = 0; j < numValues; j++) {
                set.add(data.valueAt(j));
            }
            assertThat(set, equalTo(v));

            final LongHashSet doubleV = new LongHashSet();
            for (LongCursor c : v) {
                doubleV.add(Double.doubleToLongBits(c.value));
            }
            doubleSet.clear();
            doubleData.setDocument(i);
            numValues = doubleData.count();
            double prev = 0;
            for (int j = 0; j < numValues; j++) {
                double current = doubleData.valueAt(j);
                doubleSet.add(Double.doubleToLongBits(current));
                if (j > 0) {
                    assertThat(prev, lessThan(current));
                }
                prev = current;
            }
            assertThat(doubleSet, equalTo(doubleV));
        }
    }

    private void test(Data data) throws Exception {
        Random r = getRandom();
        final int numDocs = 1000 + r.nextInt(19000);
        final List<LongHashSet> values = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; ++i) {
            final int numValues = data.numValues(r);
            final LongHashSet vals = new LongHashSet(numValues);
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
