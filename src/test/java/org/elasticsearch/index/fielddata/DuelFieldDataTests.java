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

import com.google.common.collect.Lists;
import org.apache.lucene.document.*;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.DistanceUnit.Distance;
import org.junit.Test;

import java.util.*;
import java.util.Map.Entry;

import static org.hamcrest.Matchers.*;

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
            IndexFieldData<?> leftFieldData = getForField(left.getKey(), left.getValue().name().toLowerCase(Locale.ROOT));
            ifdService.clear();
            IndexFieldData<?> rightFieldData = getForField(right.getKey(), right.getValue().name().toLowerCase(Locale.ROOT));
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
            IndexNumericFieldData<?> leftFieldData = getForField(left.getKey(), left.getValue().name().toLowerCase(Locale.ROOT));
            ifdService.clear();
            IndexNumericFieldData<?> rightFieldData = getForField(right.getKey(), right.getValue().name().toLowerCase(Locale.ROOT));

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
            IndexNumericFieldData<?> leftFieldData = getForField(left.getKey(), left.getValue().name().toLowerCase(Locale.ROOT));

            ifdService.clear();
            IndexNumericFieldData<?> rightFieldData = getForField(right.getKey(), right.getValue().name().toLowerCase(Locale.ROOT));

            assertOrder(left.getValue().order(), leftFieldData, context);
            assertOrder(right.getValue().order(), rightFieldData, context);
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
            IndexFieldData<?> leftFieldData = getForField(left.getKey(), left.getValue().name().toLowerCase(Locale.ROOT));

            ifdService.clear();
            IndexFieldData<?> rightFieldData = getForField(right.getKey(), right.getValue().name().toLowerCase(Locale.ROOT));

            duelFieldDataBytes(random, context, leftFieldData, rightFieldData, pre);
            duelFieldDataBytes(random, context, rightFieldData, leftFieldData, pre);

            DirectoryReader perSegment = DirectoryReader.open(writer, true);
            CompositeReaderContext composite = perSegment.getContext();
            List<AtomicReaderContext> leaves = composite.leaves();
            for (AtomicReaderContext atomicReaderContext : leaves) {
                assertOrder(AtomicFieldData.Order.BYTES, leftFieldData, atomicReaderContext);
                assertOrder(AtomicFieldData.Order.BYTES, rightFieldData, atomicReaderContext);
                duelFieldDataBytes(random, atomicReaderContext, leftFieldData, rightFieldData, pre);
            }
            perSegment.close();
        }

    }

    public void testDuelGeoPoints() throws Exception {
        Random random = getRandom();
        int atLeast = atLeast(random, 1000);
        int maxValuesPerDoc = randomIntBetween(1, 3);
        for (int i = 0; i < atLeast; i++) {
            Document d = new Document();
            d.add(new StringField("_id", "" + i, Field.Store.NO));
            final int numValues = randomInt(maxValuesPerDoc);
            for (int j = 0; j < numValues; ++j) {
                final double lat = randomDouble() * 180 - 90;
                final double lon = randomDouble() * 360 - 180;
                d.add(new StringField("geopoint", lat + "," + lon, Field.Store.NO));
            }
            writer.addDocument(d);
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<FieldDataType, DuelFieldDataTests.Type>();
        final Distance precision = new Distance(1, randomFrom(DistanceUnit.values()));
        typeMap.put(new FieldDataType("geo_point", ImmutableSettings.builder().put("format", "array")), Type.GeoPoint);
        typeMap.put(new FieldDataType("geo_point", ImmutableSettings.builder().put("format", "compressed").put("precision", precision)), Type.GeoPoint);

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
            IndexGeoPointFieldData<?> leftFieldData = getForField(left.getKey(), left.getValue().name().toLowerCase(Locale.ROOT));

            ifdService.clear();
            IndexGeoPointFieldData<?> rightFieldData = getForField(right.getKey(), right.getValue().name().toLowerCase(Locale.ROOT));

            duelFieldDataGeoPoint(random, context, leftFieldData, rightFieldData, precision);
            duelFieldDataGeoPoint(random, context, rightFieldData, leftFieldData, precision);

            DirectoryReader perSegment = DirectoryReader.open(writer, true);
            CompositeReaderContext composite = perSegment.getContext();
            List<AtomicReaderContext> leaves = composite.leaves();
            for (AtomicReaderContext atomicReaderContext : leaves) {
                duelFieldDataGeoPoint(random, atomicReaderContext, leftFieldData, rightFieldData, precision);
            }
            perSegment.close();
        }
    }

    private void assertOrder(AtomicFieldData.Order order, IndexFieldData<?> data, AtomicReaderContext context) throws Exception {
        AtomicFieldData<?> leftData = randomBoolean() ? data.load(context) : data.loadDirect(context);
        assertThat(leftData.getBytesValues(randomBoolean()).getOrder(), is(order));
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
            BytesRef previous = null;
            for (int j = 0; j < numValues; j++) {

                rightSpare.copyBytes(rightBytesValues.nextValue());
                leftSpare.copyBytes(leftBytesValues.nextValue());
                assertThat(rightSpare.hashCode(), equalTo(rightBytesValues.currentValueHash()));
                assertThat(leftSpare.hashCode(), equalTo(leftBytesValues.currentValueHash()));
                if (previous != null && leftBytesValues.getOrder() == rightBytesValues.getOrder()) { // we can only compare the
                  assertThat(pre.compare(previous, rightSpare), lessThan(0));
                }
                previous = BytesRef.deepCopyOf(rightSpare);
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
            double previous = 0;
            for (int j = 0; j < numValues; j++) {
                double current;
                assertThat(leftDoubleValues.nextValue(), equalTo(current = rightDoubleValues.nextValue()));
                if (j > 0) {
                   assertThat(Double.compare(previous,current), lessThan(0));
                }
                previous = current;
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
            long previous = 0;
            assertThat((numValues = leftLongValues.setDocument(i)), equalTo(rightLongValues.setDocument(i)));
            for (int j = 0; j < numValues; j++) {
                long current;
                assertThat(leftLongValues.nextValue(), equalTo(current = rightLongValues.nextValue()));
                if (j > 0) {
                    assertThat(previous, lessThan(current));
                }
                previous = current;
            }
        }
    }

    private static void duelFieldDataGeoPoint(Random random, AtomicReaderContext context, IndexGeoPointFieldData<?> left, IndexGeoPointFieldData<?> right, Distance precision) throws Exception {
        AtomicGeoPointFieldData<?> leftData = random.nextBoolean() ? left.load(context) : left.loadDirect(context);
        AtomicGeoPointFieldData<?> rightData = random.nextBoolean() ? right.load(context) : right.loadDirect(context);

        assertThat(leftData.getNumDocs(), equalTo(rightData.getNumDocs()));

        int numDocs = leftData.getNumDocs();
        GeoPointValues leftValues = leftData.getGeoPointValues();
        GeoPointValues rightValues = rightData.getGeoPointValues();
        for (int i = 0; i < numDocs; ++i) {
            final int numValues = leftValues.setDocument(i);
            assertEquals(numValues, rightValues.setDocument(i));
            List<GeoPoint> leftPoints = Lists.newArrayList();
            List<GeoPoint> rightPoints = Lists.newArrayList();
            for (int j = 0; j < numValues; ++j) {
                GeoPoint l = leftValues.nextValue();
                leftPoints.add(new GeoPoint(l.getLat(), l.getLon()));
                GeoPoint r = rightValues.nextValue();
                rightPoints.add(new GeoPoint(r.getLat(), r.getLon()));
            }
            for (GeoPoint l : leftPoints) {
                assertTrue("Couldn't find " + l + " among " + rightPoints, contains(l, rightPoints, precision));
            }
            for (GeoPoint r : rightPoints) {
                assertTrue("Couldn't find " + r + " among " + leftPoints, contains(r, leftPoints, precision));
            }
        }
    }

    private static boolean contains(GeoPoint point, List<GeoPoint> set, Distance precision) {
        for (GeoPoint r : set) {
            final double distance = GeoDistance.PLANE.calculate(point.getLat(), point.getLon(), r.getLat(), r.getLon(), DistanceUnit.METERS);
            if (new Distance(distance, DistanceUnit.METERS).compareTo(precision) <= 0) {
                return true;
            }
        }
        return false;
    }

    private static class Preprocessor {

        public String toString(BytesRef ref) {
            return ref.utf8ToString();
        }

        public int compare(BytesRef a, BytesRef b) {
            return a.compareTo(b);
        }
    }

    private static class ToDoublePreprocessor extends Preprocessor {

        @Override
        public String toString(BytesRef ref) {
            assert ref.length > 0;
            return Double.toString(Double.parseDouble(super.toString(ref)));
        }

        @Override
        public int compare(BytesRef a, BytesRef b) {
            Double _a  = Double.parseDouble(super.toString(a));
            return _a.compareTo(Double.parseDouble(super.toString(b)));
        }
    }


    private static enum Type {
        Float(AtomicFieldData.Order.NUMERIC), Double(AtomicFieldData.Order.NUMERIC), Integer(AtomicFieldData.Order.NUMERIC), Long(AtomicFieldData.Order.NUMERIC), Bytes(AtomicFieldData.Order.BYTES), GeoPoint(AtomicFieldData.Order.NONE);

        private final AtomicFieldData.Order order;
        Type(AtomicFieldData.Order order) {
            this.order = order;
        }

        public AtomicFieldData.Order order() {
            return order;
        }
    }

}

