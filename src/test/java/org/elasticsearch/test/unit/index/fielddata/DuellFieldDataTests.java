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

import org.apache.lucene.document.*;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.BytesValues.Iter;
import org.elasticsearch.index.mapper.FieldMapper;
import org.junit.Test;

import java.util.*;
import java.util.Map.Entry;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DuellFieldDataTests extends AbstractFieldDataTests {

    @Override
    protected FieldDataType getFieldDataType() {
        return null;
    }

    public static int atLeast(Random random, int i) {
        int min = i;
        int max = min + (min / 2);
        return min + random.nextInt(max - min);
    }

    @Test
    public void testDuellAllTypesSingleValue() throws Exception {
        Random random = getRandom();
        int atLeast = atLeast(random, 1000);
        for (int i = 0; i < atLeast; i++) {
            int v = (random.nextBoolean() ? -1 * random.nextInt(Byte.MAX_VALUE) : random.nextInt(Byte.MAX_VALUE));
            Document d = new Document();
            d.add(new StringField("_id", "" + i, Field.Store.NO));
            if (random.nextInt(15) != 0) {
                d.add(new LongField("long", v, Field.Store.NO));
                d.add(new IntField("integer", v, Field.Store.NO));
                d.add(new DoubleField("double", v, Field.Store.NO));
                d.add(new FloatField("float", v, Field.Store.NO));
                d.add(new StringField("bytes", "" + v, Field.Store.NO));
            }
            writer.addDocument(d);
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<FieldDataType, DuellFieldDataTests.Type>();
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "fst")), Type.Bytes);
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")), Type.Bytes);
        typeMap.put(new FieldDataType("byte"), Type.Integer);
        typeMap.put(new FieldDataType("short"), Type.Integer);
        typeMap.put(new FieldDataType("int"), Type.Integer);
        typeMap.put(new FieldDataType("long"), Type.Long);
        typeMap.put(new FieldDataType("double"), Type.Double);
        typeMap.put(new FieldDataType("float"), Type.Float);
        ArrayList<Entry<FieldDataType, Type>> list = new ArrayList<Entry<FieldDataType, Type>>(typeMap.entrySet());
        Preprocessor pre = new ToDoublePreprocessor();
        while (!list.isEmpty()) {
            Entry<FieldDataType, Type> left;
            Entry<FieldDataType, Type> right;
            if (list.size() > 1) {
                left = list.remove(random.nextInt(list.size()));
                right = list.remove(random.nextInt(list.size()));
            } else {
                right = left = list.remove(0);
            }
            ifdService.clear();
            IndexFieldData leftFieldData = ifdService.getForField(new FieldMapper.Names(left.getValue().name().toLowerCase(Locale.ROOT)),
                    left.getKey());
            ifdService.clear();
            IndexFieldData rightFieldData = ifdService.getForField(new FieldMapper.Names(right.getValue().name().toLowerCase(Locale.ROOT)),
                    right.getKey());
            duellFieldDataBytes(random, context, leftFieldData, rightFieldData, pre);
            duellFieldDataBytes(random, context, rightFieldData, leftFieldData, pre);

            DirectoryReader perSegment = DirectoryReader.open(writer, true);
            CompositeReaderContext composite = perSegment.getContext();
            List<AtomicReaderContext> leaves = composite.leaves();
            for (AtomicReaderContext atomicReaderContext : leaves) {
                duellFieldDataBytes(random, atomicReaderContext, leftFieldData, rightFieldData, pre);
            }
        }
    }


    @Test
    public void testDuellIntegers() throws Exception {
        Random random = getRandom();
        int atLeast = atLeast(random, 1000);
        for (int i = 0; i < atLeast; i++) {
            Document d = new Document();
            d.add(new StringField("_id", "" + i, Field.Store.NO));
            if (random.nextInt(15) != 0) {
                int[] numbers = getNumbers(random, Byte.MAX_VALUE);
                for (int j : numbers) {
                    d.add(new LongField("long", j, Field.Store.NO));
                    d.add(new IntField("integer", j, Field.Store.NO));
                }
            }
            writer.addDocument(d);
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<FieldDataType, DuellFieldDataTests.Type>();
        typeMap.put(new FieldDataType("byte"), Type.Integer);
        typeMap.put(new FieldDataType("short"), Type.Integer);
        typeMap.put(new FieldDataType("int"), Type.Integer);
        typeMap.put(new FieldDataType("long"), Type.Long);
        ArrayList<Entry<FieldDataType, Type>> list = new ArrayList<Entry<FieldDataType, Type>>(typeMap.entrySet());
        while (!list.isEmpty()) {
            Entry<FieldDataType, Type> left;
            Entry<FieldDataType, Type> right;
            if (list.size() > 1) {
                left = list.remove(random.nextInt(list.size()));
                right = list.remove(random.nextInt(list.size()));
            } else {
                right = left = list.remove(0);
            }
            ifdService.clear();
            IndexNumericFieldData leftFieldData = ifdService.getForField(new FieldMapper.Names(left.getValue().name().toLowerCase(Locale.ROOT)),
                    left.getKey());
            ifdService.clear();
            IndexNumericFieldData rightFieldData = ifdService.getForField(new FieldMapper.Names(right.getValue().name().toLowerCase(Locale.ROOT)),
                    right.getKey());
            duellFieldDataLong(random, context, leftFieldData, rightFieldData);
            duellFieldDataLong(random, context, rightFieldData, leftFieldData);

            DirectoryReader perSegment = DirectoryReader.open(writer, true);
            CompositeReaderContext composite = perSegment.getContext();
            List<AtomicReaderContext> leaves = composite.leaves();
            for (AtomicReaderContext atomicReaderContext : leaves) {
                duellFieldDataLong(random, atomicReaderContext, leftFieldData, rightFieldData);
            }
        }

    }

    @Test
    public void testDuellDoubles() throws Exception {
        Random random = getRandom();
        int atLeast = atLeast(random, 1000);
        for (int i = 0; i < atLeast; i++) {
            Document d = new Document();
            d.add(new StringField("_id", "" + i, Field.Store.NO));
            if (random.nextInt(15) != 0) {
                int[] numbers = getNumbers(random, Short.MAX_VALUE);
                for (int j : numbers) {
                    d.add(new FloatField("float", j, Field.Store.NO));
                    d.add(new DoubleField("double", j, Field.Store.NO));
                }
            }
            writer.addDocument(d);
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<FieldDataType, DuellFieldDataTests.Type>();
        typeMap.put(new FieldDataType("float"), Type.Float);
        typeMap.put(new FieldDataType("double"), Type.Double);
        ArrayList<Entry<FieldDataType, Type>> list = new ArrayList<Entry<FieldDataType, Type>>(typeMap.entrySet());
        while (!list.isEmpty()) {
            Entry<FieldDataType, Type> left;
            Entry<FieldDataType, Type> right;
            if (list.size() > 1) {
                left = list.remove(random.nextInt(list.size()));
                right = list.remove(random.nextInt(list.size()));
            } else {
                right = left = list.remove(0);
            }
            ifdService.clear();
            IndexNumericFieldData leftFieldData = ifdService.getForField(new FieldMapper.Names(left.getValue().name().toLowerCase(Locale.ROOT)),
                    left.getKey());
            ifdService.clear();
            IndexNumericFieldData rightFieldData = ifdService.getForField(new FieldMapper.Names(right.getValue().name().toLowerCase(Locale.ROOT)),
                    right.getKey());
            duellFieldDataDouble(random, context, leftFieldData, rightFieldData);
            duellFieldDataDouble(random, context, rightFieldData, leftFieldData);

            DirectoryReader perSegment = DirectoryReader.open(writer, true);
            CompositeReaderContext composite = perSegment.getContext();
            List<AtomicReaderContext> leaves = composite.leaves();
            for (AtomicReaderContext atomicReaderContext : leaves) {
                duellFieldDataDouble(random, atomicReaderContext, leftFieldData, rightFieldData);
            }
        }

    }


    @Test
    public void testDuellStrings() throws Exception {
        Random random = getRandom();
        int atLeast = atLeast(random, 1000);
        for (int i = 0; i < atLeast; i++) {
            Document d = new Document();
            d.add(new StringField("_id", "" + i, Field.Store.NO));
            if (random.nextInt(15) != 0) {
                int[] numbers = getNumbers(random, Integer.MAX_VALUE);
                for (int j : numbers) {
                    d.add(new StringField("bytes", English.longToEnglish(j), Field.Store.NO));
                }
                if (random.nextInt(10) == 0) {
                    d.add(new StringField("bytes", "", Field.Store.NO));
                }
            }
            writer.addDocument(d);
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<FieldDataType, DuellFieldDataTests.Type>();
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "fst")), Type.Bytes);
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")), Type.Bytes);
        // TODO add filters
        ArrayList<Entry<FieldDataType, Type>> list = new ArrayList<Entry<FieldDataType, Type>>(typeMap.entrySet());
        Preprocessor pre = new Preprocessor();
        while (!list.isEmpty()) {
            Entry<FieldDataType, Type> left;
            Entry<FieldDataType, Type> right;
            if (list.size() > 1) {
                left = list.remove(random.nextInt(list.size()));
                right = list.remove(random.nextInt(list.size()));
            } else {
                right = left = list.remove(0);
            }
            ifdService.clear();
            IndexFieldData leftFieldData = ifdService.getForField(new FieldMapper.Names(left.getValue().name().toLowerCase(Locale.ROOT)),
                    left.getKey());
            ifdService.clear();
            IndexFieldData rightFieldData = ifdService.getForField(new FieldMapper.Names(right.getValue().name().toLowerCase(Locale.ROOT)),
                    right.getKey());
            duellFieldDataBytes(random, context, leftFieldData, rightFieldData, pre);
            duellFieldDataBytes(random, context, rightFieldData, leftFieldData, pre);

            DirectoryReader perSegment = DirectoryReader.open(writer, true);
            CompositeReaderContext composite = perSegment.getContext();
            List<AtomicReaderContext> leaves = composite.leaves();
            for (AtomicReaderContext atomicReaderContext : leaves) {
                duellFieldDataBytes(random, atomicReaderContext, leftFieldData, rightFieldData, pre);
            }
            perSegment.close();
        }

    }

    private int[] getNumbers(Random random, int margin) {
        if (random.nextInt(20) == 0) {
            int[] num = new int[1 + random.nextInt(10)];
            for (int i = 0; i < num.length; i++) {
                int v = (random.nextBoolean() ? -1 * random.nextInt(margin) : random.nextInt(margin));
                num[i] = v;
            }
            return num;
        }
        return new int[]{(random.nextBoolean() ? -1 * random.nextInt(margin) : random.nextInt(margin))};
    }


    private static void duellFieldDataBytes(Random random, AtomicReaderContext context, IndexFieldData left, IndexFieldData right, Preprocessor pre) throws Exception {
        AtomicFieldData leftData = random.nextBoolean() ? left.load(context) : left.loadDirect(context);
        AtomicFieldData rightData = random.nextBoolean() ? right.load(context) : right.loadDirect(context);
        assertThat(leftData.isMultiValued(), equalTo(rightData.isMultiValued()));
        assertThat(leftData.getNumDocs(), equalTo(rightData.getNumDocs()));

        int numDocs = leftData.getNumDocs();
        BytesValues leftBytesValues = random.nextBoolean() ? leftData.getBytesValues() : leftData.getHashedBytesValues();
        BytesValues rightBytesValues = random.nextBoolean() ? rightData.getBytesValues() : rightData.getHashedBytesValues();
        for (int i = 0; i < numDocs; i++) {
            assertThat(leftBytesValues.hasValue(i), equalTo(rightBytesValues.hasValue(i)));
            if (leftBytesValues.hasValue(i)) {
                assertThat(pre.toString(leftBytesValues.getValue(i)), equalTo(pre.toString(rightBytesValues.getValue(i))));

            } else {
                assertThat(leftBytesValues.getValue(i), nullValue());
                assertThat(rightBytesValues.getValue(i), nullValue());
            }

            boolean hasValue = leftBytesValues.hasValue(i);
            Iter leftIter = leftBytesValues.getIter(i);
            Iter rightIter = rightBytesValues.getIter(i);
            assertThat(leftIter.hasNext(), equalTo(rightIter.hasNext()));
            assertThat(leftIter.hasNext(), equalTo(hasValue));

            while (leftIter.hasNext()) {
                assertThat(hasValue, equalTo(true));
                assertThat(leftIter.hasNext(), equalTo(rightIter.hasNext()));
                BytesRef rightBytes = rightIter.next();
                BytesRef leftBytes = leftIter.next();

                assertThat(pre.toString(leftBytes), equalTo(pre.toString(rightBytes)));
                if (rightBytes.equals(leftBytes)) {
                    assertThat(leftIter.hash(), equalTo(rightIter.hash()));// call twice
                    assertThat(leftIter.hash(), equalTo(rightIter.hash()));
                    assertThat(leftIter.hash(), equalTo(rightBytes.hashCode()));
                    assertThat(rightIter.hash(), equalTo(leftBytes.hashCode()));
                }
            }
            assertThat(leftIter.hasNext(), equalTo(rightIter.hasNext()));
        }
    }


    private static void duellFieldDataDouble(Random random, AtomicReaderContext context, IndexNumericFieldData left, IndexNumericFieldData right) throws Exception {
        AtomicNumericFieldData leftData = random.nextBoolean() ? left.load(context) : left.loadDirect(context);
        AtomicNumericFieldData rightData = random.nextBoolean() ? right.load(context) : right.loadDirect(context);

        assertThat(leftData.isMultiValued(), equalTo(rightData.isMultiValued()));
        assertThat(leftData.getNumDocs(), equalTo(rightData.getNumDocs()));

        int numDocs = leftData.getNumDocs();
        DoubleValues leftDoubleValues = leftData.getDoubleValues();
        DoubleValues rightDoubleValues = rightData.getDoubleValues();
        for (int i = 0; i < numDocs; i++) {
            assertThat(leftDoubleValues.hasValue(i), equalTo(rightDoubleValues.hasValue(i)));
            if (leftDoubleValues.hasValue(i)) {
                assertThat(leftDoubleValues.getValue(i), equalTo(rightDoubleValues.getValue(i)));

            } else {
                assertThat(leftDoubleValues.getValue(i), equalTo(0d));
                assertThat(rightDoubleValues.getValue(i), equalTo(0d));
            }

            boolean hasValue = leftDoubleValues.hasValue(i);
            DoubleValues.Iter leftIter = leftDoubleValues.getIter(i);
            DoubleValues.Iter rightIter = rightDoubleValues.getIter(i);
            assertThat(leftIter.hasNext(), equalTo(rightIter.hasNext()));
            assertThat(leftIter.hasNext(), equalTo(hasValue));

            while (leftIter.hasNext()) {
                assertThat(hasValue, equalTo(true));
                assertThat(leftIter.hasNext(), equalTo(rightIter.hasNext()));
                double rightValue = rightIter.next();
                double leftValue = leftIter.next();

                assertThat(leftValue, equalTo(rightValue));
            }
            assertThat(leftIter.hasNext(), equalTo(rightIter.hasNext()));
        }
    }

    private static void duellFieldDataLong(Random random, AtomicReaderContext context, IndexNumericFieldData left, IndexNumericFieldData right) throws Exception {
        AtomicNumericFieldData leftData = random.nextBoolean() ? left.load(context) : left.loadDirect(context);
        AtomicNumericFieldData rightData = random.nextBoolean() ? right.load(context) : right.loadDirect(context);

        assertThat(leftData.isMultiValued(), equalTo(rightData.isMultiValued()));
        assertThat(leftData.getNumDocs(), equalTo(rightData.getNumDocs()));

        int numDocs = leftData.getNumDocs();
        LongValues leftDoubleValues = leftData.getLongValues();
        LongValues rightDoubleValues = rightData.getLongValues();
        for (int i = 0; i < numDocs; i++) {
            assertThat(leftDoubleValues.hasValue(i), equalTo(rightDoubleValues.hasValue(i)));
            if (leftDoubleValues.hasValue(i)) {
                assertThat(leftDoubleValues.getValue(i), equalTo(rightDoubleValues.getValue(i)));

            } else {
                assertThat(leftDoubleValues.getValue(i), equalTo(0l));
                assertThat(rightDoubleValues.getValue(i), equalTo(0l));
            }

            boolean hasValue = leftDoubleValues.hasValue(i);
            LongValues.Iter leftIter = leftDoubleValues.getIter(i);
            LongValues.Iter rightIter = rightDoubleValues.getIter(i);
            assertThat(leftIter.hasNext(), equalTo(rightIter.hasNext()));
            assertThat(leftIter.hasNext(), equalTo(hasValue));

            while (leftIter.hasNext()) {
                assertThat(hasValue, equalTo(true));
                assertThat(leftIter.hasNext(), equalTo(rightIter.hasNext()));
                long rightValue = rightIter.next();
                long leftValue = leftIter.next();

                assertThat(leftValue, equalTo(rightValue));
            }
            assertThat(leftIter.hasNext(), equalTo(rightIter.hasNext()));
        }
    }


    private static class Preprocessor {

        public String toString(BytesRef ref) {
            return ref.utf8ToString();
        }
    }

    private static class ToDoublePreprocessor extends Preprocessor {
        public String toString(BytesRef ref) {
            return Double.toString(Double.parseDouble(super.toString(ref)));
        }
    }


    private static enum Type {
        Float, Double, Integer, Long, Bytes
    }
}
