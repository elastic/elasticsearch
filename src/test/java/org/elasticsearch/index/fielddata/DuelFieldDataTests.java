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

import org.apache.lucene.document.*;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.junit.Test;

import java.util.*;
import java.util.Map.Entry;

import static org.hamcrest.Matchers.equalTo;

public class DuelFieldDataTests extends AbstractFieldDataTests {

    @Override
    protected FieldDataType getFieldDataType() {
        return null;
    }

    public static int atLeast(Random random, int i) {
        int min = i;
        int max = min + (min / 2);
        return min + random.nextInt(max - min);
    }

    private static SortedSetDocValuesField longDV(String name, long l) {
        final BytesRef bytes = new BytesRef();
        NumericUtils.longToPrefixCodedBytes(l, 0, bytes);
        return new SortedSetDocValuesField(name, bytes);
    }

    private static SortedSetDocValuesField intDV(String name, int i) {
        final BytesRef bytes = new BytesRef();
        NumericUtils.intToPrefixCodedBytes(i, 0, bytes);
        return new SortedSetDocValuesField(name, bytes);
    }

    private static SortedSetDocValuesField floatDV(String name, float f) {
        return intDV(name, NumericUtils.floatToSortableInt(f));
    }

    private static SortedSetDocValuesField doubleDV(String name, double f) {
        return longDV(name, NumericUtils.doubleToSortableLong(f));
    }

    @Test
    public void testDuelAllTypesSingleValue() throws Exception {
        Random random = getRandom();
        int atLeast = atLeast(random, 1000);
        for (int i = 0; i < atLeast; i++) {
            int v = (random.nextBoolean() ? -1 * random.nextInt(Byte.MAX_VALUE) : random.nextInt(Byte.MAX_VALUE));
            Document d = new Document();
            d.add(new StringField("_id", "" + i, Field.Store.NO));
            if (random.nextInt(15) != 0) {
                d.add(new LongField("long", v, Field.Store.NO ));
                d.add(new IntField("integer", v, Field.Store.NO));
                d.add(new DoubleField("double", v, Field.Store.NO));
                d.add(new FloatField("float", v, Field.Store.NO));
                d.add(new StringField("bytes","" + v, Field.Store.NO));
                if (LuceneTestCase.defaultCodecSupportsSortedSet()) {
                    d.add(longDV("long", v));
                    d.add(intDV("integer", v));
                    d.add(doubleDV("double", v));
                    d.add(floatDV("float", v));
                    d.add(new SortedSetDocValuesField("bytes", new BytesRef("" + v)));
                }
            }
            writer.addDocument(d);
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<FieldDataType, DuelFieldDataTests.Type>();
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "fst")), Type.Bytes);
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")), Type.Bytes);
        typeMap.put(new FieldDataType("byte", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("short", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("int", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("long", ImmutableSettings.builder().put("format", "array")), Type.Long);
        typeMap.put(new FieldDataType("double", ImmutableSettings.builder().put("format", "array")), Type.Double);
        typeMap.put(new FieldDataType("float", ImmutableSettings.builder().put("format", "array")), Type.Float);
        if (LuceneTestCase.defaultCodecSupportsSortedSet()) {
            typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "doc_values")), Type.Bytes);
            typeMap.put(new FieldDataType("byte", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
            typeMap.put(new FieldDataType("short", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
            typeMap.put(new FieldDataType("int", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
            typeMap.put(new FieldDataType("long", ImmutableSettings.builder().put("format", "doc_values")), Type.Long);
            typeMap.put(new FieldDataType("double", ImmutableSettings.builder().put("format", "doc_values")), Type.Double);
            typeMap.put(new FieldDataType("float", ImmutableSettings.builder().put("format", "doc_values")), Type.Float);
        }
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
            IndexFieldData leftFieldData = ifdService.getForField(new FieldMapper.Names(left.getValue().name().toLowerCase(Locale.ROOT)), left.getKey(), true);
            ifdService.clear();
            IndexFieldData rightFieldData = ifdService.getForField(new FieldMapper.Names(right.getValue().name().toLowerCase(Locale.ROOT)), right.getKey(), true);
            duelFieldDataBytes(random, context, leftFieldData, rightFieldData, pre);
            duelFieldDataBytes(random, context, rightFieldData, leftFieldData, pre);

            DirectoryReader perSegment = DirectoryReader.open(writer, true);
            CompositeReaderContext composite = perSegment.getContext();
            List<AtomicReaderContext> leaves = composite.leaves();
            for (AtomicReaderContext atomicReaderContext : leaves) {
                duelFieldDataBytes(random, atomicReaderContext, leftFieldData, rightFieldData, pre);
            }
        }
    }


    @Test
    public void testDuelIntegers() throws Exception {
        Random random = getRandom();
        int atLeast = atLeast(random, 1000);
        for (int i = 0; i < atLeast; i++) {
            Document d = new Document();
            d.add(new StringField("_id", "" + i, Field.Store.NO));
            if (random.nextInt(15) != 0) {
                int[] numbers = getNumbers(random, Byte.MAX_VALUE);
                for (int j : numbers) {
                    d.add(new LongField("long", j, Field.Store.NO ));
                    d.add(new IntField("integer", j, Field.Store.NO));
                    if (LuceneTestCase.defaultCodecSupportsSortedSet()) {
                        d.add(longDV("long", j));
                        d.add(intDV("integer", j));
                    }
                }
            }
            writer.addDocument(d);
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<FieldDataType, Type>();
        typeMap.put(new FieldDataType("byte", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("short", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("int", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("long", ImmutableSettings.builder().put("format", "array")), Type.Long);
        if (LuceneTestCase.defaultCodecSupportsSortedSet()) {
            typeMap.put(new FieldDataType("byte", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
            typeMap.put(new FieldDataType("short", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
            typeMap.put(new FieldDataType("int", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
            typeMap.put(new FieldDataType("long", ImmutableSettings.builder().put("format", "doc_values")), Type.Long);
        }
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
                    left.getKey(), true);
            ifdService.clear();
            IndexNumericFieldData rightFieldData = ifdService.getForField(new FieldMapper.Names(right.getValue().name().toLowerCase(Locale.ROOT)),
                    right.getKey(), true);
            duelFieldDataLong(random, context, leftFieldData, rightFieldData);
            duelFieldDataLong(random, context, rightFieldData, leftFieldData);

            DirectoryReader perSegment = DirectoryReader.open(writer, true);
            CompositeReaderContext composite = perSegment.getContext();
            List<AtomicReaderContext> leaves = composite.leaves();
            for (AtomicReaderContext atomicReaderContext : leaves) {
                duelFieldDataLong(random, atomicReaderContext, leftFieldData, rightFieldData);
            }
        }

    }

    @Test
    public void testDuelDoubles() throws Exception {
        Random random = getRandom();
        int atLeast = atLeast(random, 1000);
        for (int i = 0; i < atLeast; i++) {
            Document d = new Document();
            d.add(new StringField("_id", "" + i, Field.Store.NO));
            if (random.nextInt(15) != 0) {
                int[] numbers = getNumbers(random, Short.MAX_VALUE);
                for (int j : numbers) {
                    d.add(new FloatField("float", j, Field.Store.NO ));
                    d.add(new DoubleField("double", j, Field.Store.NO));
                    if (LuceneTestCase.defaultCodecSupportsSortedSet()) {
                        d.add(doubleDV("double", j));
                        d.add(floatDV("float", j));
                    }
                }
            }
            writer.addDocument(d);
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<FieldDataType, Type>();
        typeMap.put(new FieldDataType("double", ImmutableSettings.builder().put("format", "array")), Type.Double);
        typeMap.put(new FieldDataType("float", ImmutableSettings.builder().put("format", "array")), Type.Float);
        if (LuceneTestCase.defaultCodecSupportsSortedSet()) {
            typeMap.put(new FieldDataType("double", ImmutableSettings.builder().put("format", "doc_values")), Type.Double);
            typeMap.put(new FieldDataType("float", ImmutableSettings.builder().put("format", "doc_values")), Type.Float);
        }
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
                    left.getKey(), true);
            ifdService.clear();
            IndexNumericFieldData rightFieldData = ifdService.getForField(new FieldMapper.Names(right.getValue().name().toLowerCase(Locale.ROOT)),
                    right.getKey(), true);
            duelFieldDataDouble(random, context, leftFieldData, rightFieldData);
            duelFieldDataDouble(random, context, rightFieldData, leftFieldData);

            DirectoryReader perSegment = DirectoryReader.open(writer, true);
            CompositeReaderContext composite = perSegment.getContext();
            List<AtomicReaderContext> leaves = composite.leaves();
            for (AtomicReaderContext atomicReaderContext : leaves) {
                duelFieldDataDouble(random, atomicReaderContext, leftFieldData, rightFieldData);
            }
        }

    }


    @Test
    public void testDuelStrings() throws Exception {
        Random random = getRandom();
        int atLeast = atLeast(random, 1000);
        for (int i = 0; i < atLeast; i++) {
            Document d = new Document();
            d.add(new StringField("_id", "" + i, Field.Store.NO));
            if (random.nextInt(15) != 0) {
                int[] numbers = getNumbers(random, Integer.MAX_VALUE);
                for (int j : numbers) {
                    final String s = English.longToEnglish(j);
                    d.add(new StringField("bytes", s, Field.Store.NO));
                    if (LuceneTestCase.defaultCodecSupportsSortedSet()) {
                        d.add(new SortedSetDocValuesField("bytes", new BytesRef(s)));
                    }
                }
                if (random.nextInt(10) == 0) {
                    d.add(new StringField("bytes", "", Field.Store.NO));
                    if (LuceneTestCase.defaultCodecSupportsSortedSet()) {
                        d.add(new SortedSetDocValuesField("bytes", new BytesRef()));
                    }
                }
            }
            writer.addDocument(d);
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<FieldDataType, DuelFieldDataTests.Type>();
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "fst")), Type.Bytes);
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")), Type.Bytes);
        if (LuceneTestCase.defaultCodecSupportsSortedSet()) {
            typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "doc_values")), Type.Bytes);
        }
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
                    left.getKey(), true);
            ifdService.clear();
            IndexFieldData rightFieldData = ifdService.getForField(new FieldMapper.Names(right.getValue().name().toLowerCase(Locale.ROOT)),
                    right.getKey(), true);
            duelFieldDataBytes(random, context, leftFieldData, rightFieldData, pre);
            duelFieldDataBytes(random, context, rightFieldData, leftFieldData, pre);

            DirectoryReader perSegment = DirectoryReader.open(writer, true);
            CompositeReaderContext composite = perSegment.getContext();
            List<AtomicReaderContext> leaves = composite.leaves();
            for (AtomicReaderContext atomicReaderContext : leaves) {
                duelFieldDataBytes(random, atomicReaderContext, leftFieldData, rightFieldData, pre);
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


    private static void duelFieldDataBytes(Random random, AtomicReaderContext context, IndexFieldData<?> left, IndexFieldData<?> right, Preprocessor pre) throws Exception {
        AtomicFieldData<?> leftData = random.nextBoolean() ? left.load(context) : left.loadDirect(context);
        AtomicFieldData<?> rightData = random.nextBoolean() ? right.load(context) : right.loadDirect(context);
        assertThat(leftData.getNumDocs(), equalTo(rightData.getNumDocs()));

        int numDocs = leftData.getNumDocs();
        BytesValues leftBytesValues = leftData.getBytesValues(random.nextBoolean());
        BytesValues rightBytesValues = rightData.getBytesValues(random.nextBoolean());
        BytesRef leftSpare = new BytesRef();
        BytesRef rightSpare = new BytesRef();
        for (int i = 0; i < numDocs; i++) {
            int numValues = 0;
            assertThat((numValues = leftBytesValues.setDocument(i)), equalTo(rightBytesValues.setDocument(i)));
            for (int j = 0; j < numValues; j++) {
                rightSpare.copyBytes(rightBytesValues.nextValue());
                leftSpare.copyBytes(leftBytesValues.nextValue());
                assertThat(rightSpare.hashCode(), equalTo(rightBytesValues.currentValueHash()));
                assertThat(leftSpare.hashCode(), equalTo(leftBytesValues.currentValueHash()));
                pre.toString(rightSpare);
                pre.toString(leftSpare);
                assertThat(pre.toString(leftSpare), equalTo(pre.toString(rightSpare)));
                if (leftSpare.equals(rightSpare)) {
                    assertThat(leftBytesValues.currentValueHash(), equalTo(rightBytesValues.currentValueHash()));
                }
            }
        }
    }


    private static void duelFieldDataDouble(Random random, AtomicReaderContext context, IndexNumericFieldData<?> left, IndexNumericFieldData<?> right) throws Exception {
        AtomicNumericFieldData leftData = random.nextBoolean() ? left.load(context) : left.loadDirect(context);
        AtomicNumericFieldData rightData = random.nextBoolean() ? right.load(context) : right.loadDirect(context);

        assertThat(leftData.getNumDocs(), equalTo(rightData.getNumDocs()));

        int numDocs = leftData.getNumDocs();
        DoubleValues leftDoubleValues = leftData.getDoubleValues();
        DoubleValues rightDoubleValues = rightData.getDoubleValues();
        for (int i = 0; i < numDocs; i++) {
            int numValues = 0;
            assertThat((numValues = leftDoubleValues.setDocument(i)), equalTo(rightDoubleValues.setDocument(i)));
            for (int j = 0; j < numValues; j++) {
                assertThat(leftDoubleValues.nextValue(), equalTo(rightDoubleValues.nextValue()));
            }
        }
    }

    private static void duelFieldDataLong(Random random, AtomicReaderContext context, IndexNumericFieldData left, IndexNumericFieldData right) throws Exception {
        AtomicNumericFieldData leftData = random.nextBoolean() ? left.load(context) : left.loadDirect(context);
        AtomicNumericFieldData rightData = random.nextBoolean() ? right.load(context) : right.loadDirect(context);

        assertThat(leftData.getNumDocs(), equalTo(rightData.getNumDocs()));

        int numDocs = leftData.getNumDocs();
        LongValues leftLongValues = leftData.getLongValues();
        LongValues rightLongValues = rightData.getLongValues();
        for (int i = 0; i < numDocs; i++) {
            int numValues = 0;
            assertThat((numValues = leftLongValues.setDocument(i)), equalTo(rightLongValues.setDocument(i)));
            for (int j = 0; j < numValues; j++) {
                assertThat(leftLongValues.nextValue(), equalTo(rightLongValues.nextValue()));
            }
        }
    }


    private static class Preprocessor {

        public String toString(BytesRef ref) {
            return ref.utf8ToString();
        }
    }

    private static class ToDoublePreprocessor extends Preprocessor {
        public String toString(BytesRef ref) {
            assert ref.length > 0;
            return Double.toString(Double.parseDouble(super.toString(ref)));
        }
    }


    private static enum Type {
        Float, Double, Integer, Long, Bytes
    }

}

