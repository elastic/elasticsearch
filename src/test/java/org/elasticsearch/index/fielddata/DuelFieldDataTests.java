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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.collect.Lists;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.DistanceUnit.Distance;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.junit.Test;

import java.util.*;
import java.util.Map.Entry;

import static org.hamcrest.Matchers.*;

public class DuelFieldDataTests extends AbstractFieldDataTests {

    @Override
    protected FieldDataType getFieldDataType() {
        return null;
    }

    @Test
    public void testDuelAllTypesSingleValue() throws Exception {
        final String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("bytes").field("type", "string").field("index", "not_analyzed").startObject("fielddata").field("format", LuceneTestCase.defaultCodecSupportsSortedSet() ? "doc_values" : "fst").endObject().endObject()
                    .startObject("byte").field("type", "byte").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                    .startObject("short").field("type", "short").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                    .startObject("integer").field("type", "integer").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                    .startObject("long").field("type", "long").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                    .startObject("float").field("type", "float").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                    .startObject("double").field("type", "double").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                .endObject().endObject().endObject().string();
        final DocumentMapper mapper = mapperService.documentMapperParser().parse(mapping);
        Random random = getRandom();
        int atLeast = scaledRandomIntBetween(1000, 1500);
        for (int i = 0; i < atLeast; i++) {
            String s = Integer.toString(randomByte());

            XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
            for (String fieldName : Arrays.asList("bytes", "byte", "short", "integer", "long", "float", "double")) {
                doc = doc.field(fieldName, s);
            }

            doc = doc.endObject();

            final ParsedDocument d = mapper.parse("type", Integer.toString(i), doc.bytes());

            writer.addDocument(d.rootDoc());

            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<>();
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "fst")), Type.Bytes);
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")), Type.Bytes);
        typeMap.put(new FieldDataType("byte", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("short", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("int", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("long", ImmutableSettings.builder().put("format", "array")), Type.Long);
        typeMap.put(new FieldDataType("double", ImmutableSettings.builder().put("format", "array")), Type.Double);
        typeMap.put(new FieldDataType("float", ImmutableSettings.builder().put("format", "array")), Type.Float);
        typeMap.put(new FieldDataType("byte", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
        typeMap.put(new FieldDataType("short", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
        typeMap.put(new FieldDataType("int", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
        typeMap.put(new FieldDataType("long", ImmutableSettings.builder().put("format", "doc_values")), Type.Long);
        typeMap.put(new FieldDataType("double", ImmutableSettings.builder().put("format", "doc_values")), Type.Double);
        typeMap.put(new FieldDataType("float", ImmutableSettings.builder().put("format", "doc_values")), Type.Float);
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "doc_values")), Type.Bytes);
        ArrayList<Entry<FieldDataType, Type>> list = new ArrayList<>(typeMap.entrySet());
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
        final String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("byte").field("type", "byte").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                    .startObject("short").field("type", "short").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                    .startObject("integer").field("type", "integer").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                    .startObject("long").field("type", "long").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                .endObject().endObject().endObject().string();

        final DocumentMapper mapper = mapperService.documentMapperParser().parse(mapping);
        Random random = getRandom();
        int atLeast = scaledRandomIntBetween(1000, 1500);
        final int maxNumValues = randomBoolean() ? 1 : randomIntBetween(2, 40);
        byte[] values = new byte[maxNumValues];
        for (int i = 0; i < atLeast; i++) {
            int numValues = randomInt(maxNumValues);
            // FD loses values if they are duplicated, so we must deduplicate for this test
            Set<Byte> vals = new HashSet<Byte>();
            for (int j = 0; j < numValues; ++j) {
                vals.add(randomByte());
            }
            
            numValues = vals.size();
            int upto = 0;
            for (Byte bb : vals) {
                values[upto++] = bb.byteValue();
            }

            XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
            for (String fieldName : Arrays.asList("byte", "short", "integer", "long")) {
                doc = doc.startArray(fieldName);
                for (int j = 0; j < numValues; ++j) {
                    doc = doc.value(values[j]);
                }
                doc = doc.endArray();
            }
            doc = doc.endObject();

            final ParsedDocument d = mapper.parse("type", Integer.toString(i), doc.bytes());

            writer.addDocument(d.rootDoc());
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<>();
        typeMap.put(new FieldDataType("byte", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("short", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("int", ImmutableSettings.builder().put("format", "array")), Type.Integer);
        typeMap.put(new FieldDataType("long", ImmutableSettings.builder().put("format", "array")), Type.Long);
        typeMap.put(new FieldDataType("byte", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
        typeMap.put(new FieldDataType("short", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
        typeMap.put(new FieldDataType("int", ImmutableSettings.builder().put("format", "doc_values")), Type.Integer);
        typeMap.put(new FieldDataType("long", ImmutableSettings.builder().put("format", "doc_values")), Type.Long);
        ArrayList<Entry<FieldDataType, Type>> list = new ArrayList<>(typeMap.entrySet());
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
            IndexNumericFieldData leftFieldData = getForField(left.getKey(), left.getValue().name().toLowerCase(Locale.ROOT));
            ifdService.clear();
            IndexNumericFieldData rightFieldData = getForField(right.getKey(), right.getValue().name().toLowerCase(Locale.ROOT));

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
        final String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("float").field("type", "float").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                    .startObject("double").field("type", "double").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                .endObject().endObject().endObject().string();

        final DocumentMapper mapper = mapperService.documentMapperParser().parse(mapping);
        Random random = getRandom();
        int atLeast = scaledRandomIntBetween(1000, 1500);
        final int maxNumValues = randomBoolean() ? 1 : randomIntBetween(2, 40);
        float[] values = new float[maxNumValues];
        for (int i = 0; i < atLeast; i++) {
            int numValues = randomInt(maxNumValues);
            float def = randomBoolean() ? randomFloat() : Float.NaN;
            // FD loses values if they are duplicated, so we must deduplicate for this test
            Set<Float> vals = new HashSet<Float>();
            for (int j = 0; j < numValues; ++j) {
                if (randomBoolean()) {
                    vals.add(def);
                } else {
                    vals.add(randomFloat());
                }
            }
            numValues = vals.size();
            int upto = 0;
            for (Float f : vals) {
                values[upto++] = f.floatValue();
            }

            XContentBuilder doc = XContentFactory.jsonBuilder().startObject().startArray("float");
            for (int j = 0; j < numValues; ++j) {
                doc = doc.value(values[j]);
            }
            doc = doc.endArray().startArray("double");
            for (int j = 0; j < numValues; ++j) {
                doc = doc.value(values[j]);
            }
            doc = doc.endArray().endObject();

            final ParsedDocument d = mapper.parse("type", Integer.toString(i), doc.bytes());

            writer.addDocument(d.rootDoc());
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<>();
        typeMap.put(new FieldDataType("double", ImmutableSettings.builder().put("format", "array")), Type.Double);
        typeMap.put(new FieldDataType("float", ImmutableSettings.builder().put("format", "array")), Type.Float);
        typeMap.put(new FieldDataType("double", ImmutableSettings.builder().put("format", "doc_values")), Type.Double);
        typeMap.put(new FieldDataType("float", ImmutableSettings.builder().put("format", "doc_values")), Type.Float);
        ArrayList<Entry<FieldDataType, Type>> list = new ArrayList<>(typeMap.entrySet());
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
            IndexNumericFieldData leftFieldData = getForField(left.getKey(), left.getValue().name().toLowerCase(Locale.ROOT));

            ifdService.clear();
            IndexNumericFieldData rightFieldData = getForField(right.getKey(), right.getValue().name().toLowerCase(Locale.ROOT));

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
        int atLeast = scaledRandomIntBetween(1000, 1500);
        for (int i = 0; i < atLeast; i++) {
            Document d = new Document();
            d.add(new StringField("_id", "" + i, Field.Store.NO));
            if (random.nextInt(15) != 0) {
                int[] numbers = getNumbers(random, Integer.MAX_VALUE);
                for (int j : numbers) {
                    final String s = English.longToEnglish(j);
                    d.add(new StringField("bytes", s, Field.Store.NO));
                    d.add(new SortedSetDocValuesField("bytes", new BytesRef(s)));
                }
                if (random.nextInt(10) == 0) {
                    d.add(new StringField("bytes", "", Field.Store.NO));
                    d.add(new SortedSetDocValuesField("bytes", new BytesRef()));
                }
            }
            writer.addDocument(d);
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<>();
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "fst")), Type.Bytes);
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")), Type.Bytes);
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "doc_values")), Type.Bytes);
        // TODO add filters
        ArrayList<Entry<FieldDataType, Type>> list = new ArrayList<>(typeMap.entrySet());
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
                duelFieldDataBytes(random, atomicReaderContext, leftFieldData, rightFieldData, pre);
            }
            perSegment.close();
        }

    }

    public void testDuelGlobalOrdinals() throws Exception {
        Random random = getRandom();
        final int numDocs = scaledRandomIntBetween(10, 1000);
        final int numValues = scaledRandomIntBetween(10, 500);
        final String[] values = new String[numValues];
        for (int i = 0; i < numValues; ++i) {
            values[i] = new String(RandomStrings.randomAsciiOfLength(random, 10));
        }
        for (int i = 0; i < numDocs; i++) {
            Document d = new Document();
            final int numVals = randomInt(3);
            for (int j = 0; j < numVals; ++j) {
                final String value = RandomPicks.randomFrom(random, Arrays.asList(values));
                d.add(new StringField("string", value, Field.Store.NO));
                d.add(new SortedSetDocValuesField("bytes", new BytesRef(value)));
            }
            writer.addDocument(d);
            if (randomInt(10) == 0) {
                refreshReader();
            }
        }
        refreshReader();

        Map<FieldDataType, Type> typeMap = new HashMap<FieldDataType, DuelFieldDataTests.Type>();
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "fst")), Type.Bytes);
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")), Type.Bytes);
        typeMap.put(new FieldDataType("string", ImmutableSettings.builder().put("format", "doc_values")), Type.Bytes);

        for (Map.Entry<FieldDataType, Type> entry : typeMap.entrySet()) {
            ifdService.clear();
            IndexOrdinalsFieldData fieldData = getForField(entry.getKey(), entry.getValue().name().toLowerCase(Locale.ROOT));
            RandomAccessOrds left = fieldData.load(readerContext).getOrdinalsValues();
            fieldData.clear();
            RandomAccessOrds right = fieldData.loadGlobal(topLevelReader).load(topLevelReader.leaves().get(0)).getOrdinalsValues();
            assertEquals(left.getValueCount(), right.getValueCount());
            for (long ord = 0; ord < left.getValueCount(); ++ord) {
                assertEquals(left.lookupOrd(ord), right.lookupOrd(ord));
            }
        }
    }

    public void testDuelGeoPoints() throws Exception {
        final String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("geopoint").field("type", "geo_point").startObject("fielddata").field("format", "doc_values").endObject().endObject()
                .endObject().endObject().endObject().string();

        final DocumentMapper mapper = mapperService.documentMapperParser().parse(mapping);

        Random random = getRandom();
        int atLeast = scaledRandomIntBetween(1000, 1500);
        int maxValuesPerDoc = randomBoolean() ? 1 : randomIntBetween(2, 40);
        // to test deduplication
        double defaultLat = randomDouble() * 180 - 90;
        double defaultLon = randomDouble() * 360 - 180;
        for (int i = 0; i < atLeast; i++) {
            final int numValues = randomInt(maxValuesPerDoc);
            XContentBuilder doc = XContentFactory.jsonBuilder().startObject().startArray("geopoint");
            for (int j = 0; j < numValues; ++j) {
                if (randomBoolean()) {
                    doc.startObject().field("lat", defaultLat).field("lon", defaultLon).endObject();
                } else {
                    doc.startObject().field("lat", randomDouble() * 180 - 90).field("lon", randomDouble() * 360 - 180).endObject();
                }
            }
            doc = doc.endArray().endObject();
            final ParsedDocument d = mapper.parse("type", Integer.toString(i), doc.bytes());

            writer.addDocument(d.rootDoc());
            if (random.nextInt(10) == 0) {
                refreshReader();
            }
        }
        AtomicReaderContext context = refreshReader();
        Map<FieldDataType, Type> typeMap = new HashMap<>();
        final Distance precision = new Distance(1, randomFrom(DistanceUnit.values()));
        typeMap.put(new FieldDataType("geo_point", ImmutableSettings.builder().put("format", "array")), Type.GeoPoint);
        typeMap.put(new FieldDataType("geo_point", ImmutableSettings.builder().put("format", "compressed").put("precision", precision)), Type.GeoPoint);
        typeMap.put(new FieldDataType("geo_point", ImmutableSettings.builder().put("format", "doc_values")), Type.GeoPoint);

        ArrayList<Entry<FieldDataType, Type>> list = new ArrayList<>(typeMap.entrySet());
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
            IndexGeoPointFieldData leftFieldData = getForField(left.getKey(), left.getValue().name().toLowerCase(Locale.ROOT));

            ifdService.clear();
            IndexGeoPointFieldData rightFieldData = getForField(right.getKey(), right.getValue().name().toLowerCase(Locale.ROOT));

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
        AtomicFieldData leftData = random.nextBoolean() ? left.load(context) : left.loadDirect(context);
        AtomicFieldData rightData = random.nextBoolean() ? right.load(context) : right.loadDirect(context);

        int numDocs = context.reader().maxDoc();
        SortedBinaryDocValues leftBytesValues = leftData.getBytesValues();
        SortedBinaryDocValues rightBytesValues = rightData.getBytesValues();
        BytesRefBuilder leftSpare = new BytesRefBuilder();
        BytesRefBuilder rightSpare = new BytesRefBuilder();

        for (int i = 0; i < numDocs; i++) {
            leftBytesValues.setDocument(i);
            rightBytesValues.setDocument(i);
            int numValues = leftBytesValues.count();
            assertThat(numValues, equalTo(rightBytesValues.count()));
            BytesRef previous = null;
            for (int j = 0; j < numValues; j++) {
                rightSpare.copyBytes(rightBytesValues.valueAt(j));
                leftSpare.copyBytes(leftBytesValues.valueAt(j));
                if (previous != null) {
                    assertThat(pre.compare(previous, rightSpare.get()), lessThan(0));
                }
                previous = BytesRef.deepCopyOf(rightSpare.get());
                pre.toString(rightSpare.get());
                pre.toString(leftSpare.get());
                assertThat(pre.toString(leftSpare.get()), equalTo(pre.toString(rightSpare.get())));
            }
        }
    }


    private static void duelFieldDataDouble(Random random, AtomicReaderContext context, IndexNumericFieldData left, IndexNumericFieldData right) throws Exception {
        AtomicNumericFieldData leftData = random.nextBoolean() ? left.load(context) : left.loadDirect(context);
        AtomicNumericFieldData rightData = random.nextBoolean() ? right.load(context) : right.loadDirect(context);

        int numDocs = context.reader().maxDoc();
        SortedNumericDoubleValues leftDoubleValues = leftData.getDoubleValues();
        SortedNumericDoubleValues rightDoubleValues = rightData.getDoubleValues();
        for (int i = 0; i < numDocs; i++) {
            leftDoubleValues.setDocument(i);
            rightDoubleValues.setDocument(i);
            int numValues = leftDoubleValues.count();
            assertThat(numValues, equalTo(rightDoubleValues.count()));
            double previous = 0;
            for (int j = 0; j < numValues; j++) {
                double current = rightDoubleValues.valueAt(j);
                if (Double.isNaN(current)) {
                    assertTrue(Double.isNaN(leftDoubleValues.valueAt(j)));
                } else {
                    assertThat(leftDoubleValues.valueAt(j), closeTo(current, 0.0001));
                }
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

        int numDocs = context.reader().maxDoc();
        SortedNumericDocValues leftLongValues = leftData.getLongValues();
        SortedNumericDocValues rightLongValues = rightData.getLongValues();
        for (int i = 0; i < numDocs; i++) {
            leftLongValues.setDocument(i);
            rightLongValues.setDocument(i);
            int numValues = leftLongValues.count();
            long previous = 0;
            assertThat(numValues, equalTo(rightLongValues.count()));
            for (int j = 0; j < numValues; j++) {
                long current;
                assertThat(leftLongValues.valueAt(j), equalTo(current = rightLongValues.valueAt(j)));
                if (j > 0) {
                    assertThat(previous, lessThan(current));
                }
                previous = current;
            }
        }
    }

    private static void duelFieldDataGeoPoint(Random random, AtomicReaderContext context, IndexGeoPointFieldData left, IndexGeoPointFieldData right, Distance precision) throws Exception {
        AtomicGeoPointFieldData leftData = random.nextBoolean() ? left.load(context) : left.loadDirect(context);
        AtomicGeoPointFieldData rightData = random.nextBoolean() ? right.load(context) : right.loadDirect(context);

        int numDocs = context.reader().maxDoc();
        MultiGeoPointValues leftValues = leftData.getGeoPointValues();
        MultiGeoPointValues rightValues = rightData.getGeoPointValues();
        for (int i = 0; i < numDocs; ++i) {
            leftValues.setDocument(i);
            final int numValues = leftValues.count();
            rightValues.setDocument(i);;
            assertEquals(numValues, rightValues.count());
            List<GeoPoint> leftPoints = Lists.newArrayList();
            List<GeoPoint> rightPoints = Lists.newArrayList();
            for (int j = 0; j < numValues; ++j) {
                GeoPoint l = leftValues.valueAt(j);
                leftPoints.add(new GeoPoint(l.getLat(), l.getLon()));
                GeoPoint r = rightValues.valueAt(j);
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
            assertTrue(ref.length > 0);
            return Double.toString(Double.parseDouble(super.toString(ref)));
        }

        @Override
        public int compare(BytesRef a, BytesRef b) {
            Double _a  = Double.parseDouble(super.toString(a));
            return _a.compareTo(Double.parseDouble(super.toString(b)));
        }
    }


    private static enum Type {
        Float, Double, Integer, Long, Bytes, GeoPoint;
    }

}

