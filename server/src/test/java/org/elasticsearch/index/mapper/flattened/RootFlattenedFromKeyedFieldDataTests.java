/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.KeyFilteredSortingArrayOrderBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class RootFlattenedFromKeyedFieldDataTests extends ESTestCase {

    private enum Layout {
        SORTED_SET,
        SEPARATE_COUNT,
        ARRAY_ORDER_INLINE_NULL
    }

    private record Slot(String key, String value) {}

    private static final String KEYED_FIELD = "field._keyed";
    private static final String ROOT_FIELD = "field";

    private final Layout layout;

    public RootFlattenedFromKeyedFieldDataTests(@Name("layout") Layout layout) {
        this.layout = layout;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(Layout.values()).map(layout -> new Object[] { layout }).toList();
    }

    public void testSingleKeyedValue() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                addDoc(writer, layout, "key1\0value1");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, layout);
                    assertTrue(values.advanceExact(0));
                    assertEquals(1, values.docValueCount());
                    assertEquals(new BytesRef("value1"), values.nextValue());
                }
            }
        }
    }

    public void testMultipleKeysUniqueValues() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                addDoc(writer, layout, "key1\0alpha", "key2\0beta", "key3\0gamma");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, layout);
                    assertTrue(values.advanceExact(0));
                    assertEquals(3, values.docValueCount());
                    assertEquals(new BytesRef("alpha"), values.nextValue());
                    assertEquals(new BytesRef("beta"), values.nextValue());
                    assertEquals(new BytesRef("gamma"), values.nextValue());
                }
            }
        }
    }

    public void testDuplicateValuesAcrossKeys() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                addDoc(writer, layout, "key1\0foo", "key2\0foo", "key3\0bar");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, layout);
                    assertTrue(values.advanceExact(0));
                    assertEquals(2, values.docValueCount());
                    assertEquals(new BytesRef("bar"), values.nextValue());
                    assertEquals(new BytesRef("foo"), values.nextValue());
                }
            }
        }
    }

    public void testAllDuplicateValues() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                addDoc(writer, layout, "a\0same", "b\0same", "c\0same");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, layout);
                    assertTrue(values.advanceExact(0));
                    assertEquals(1, values.docValueCount());
                    assertEquals(new BytesRef("same"), values.nextValue());
                }
            }
        }
    }

    public void testValuesAreSorted() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                addDoc(writer, layout, "key1\0zebra", "key2\0apple", "key3\0mango");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, layout);
                    assertTrue(values.advanceExact(0));
                    assertEquals(3, values.docValueCount());
                    assertEquals(new BytesRef("apple"), values.nextValue());
                    assertEquals(new BytesRef("mango"), values.nextValue());
                    assertEquals(new BytesRef("zebra"), values.nextValue());
                }
            }
        }
    }

    public void testEmptyDocument() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                writer.addDocument(new Document());

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, layout);
                    assertFalse(values.advanceExact(0));
                }
            }
        }
    }

    public void testMultipleDocuments() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                addDoc(writer, layout, "a\0x", "b\0y");
                addDoc(writer, layout, "c\0y", "d\0z");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, layout);

                    assertTrue(values.advanceExact(0));
                    assertEquals(2, values.docValueCount());
                    List<String> doc0 = List.of(values.nextValue().utf8ToString(), values.nextValue().utf8ToString());

                    assertTrue(values.advanceExact(1));
                    assertEquals(2, values.docValueCount());
                    List<String> doc1 = List.of(values.nextValue().utf8ToString(), values.nextValue().utf8ToString());

                    assertEquals(Set.of(List.of("x", "y"), List.of("y", "z")), Set.of(doc0, doc1));
                }
            }
        }
    }

    public void testNullSlotsAreIgnored() throws IOException {
        assumeTrue("only the array order layout keeps null slots", layout == Layout.ARRAY_ORDER_INLINE_NULL);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                addArrayOrderDoc(writer, new Slot("key1", null), new Slot("key2", "value"), new Slot("key3", null));

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, layout);
                    assertTrue(values.advanceExact(0));
                    assertEquals(1, values.docValueCount());
                    assertEquals(new BytesRef("value"), values.nextValue());
                }
            }
        }
    }

    public void testDocumentsWithOnlyNullSlotsHaveNoValues() throws IOException {
        assumeTrue("only the array order layout keeps null slots", layout == Layout.ARRAY_ORDER_INLINE_NULL);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                addArrayOrderDoc(writer, new Slot("key1", null));
                addArrayOrderDoc(writer, new Slot("key1", null), new Slot("key2", null));

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, layout);
                    assertFalse(values.advanceExact(0));
                    assertFalse(values.advanceExact(1));
                }
            }
        }
    }

    public void testEmptyStringIsAValueAndNullIsNot() throws IOException {
        assumeTrue("only the array order layout keeps null slots", layout == Layout.ARRAY_ORDER_INLINE_NULL);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                addArrayOrderDoc(writer, new Slot("key1", null), new Slot("key1", ""), new Slot("key1", null), new Slot("key1", ""));

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, layout);
                    assertTrue(values.advanceExact(0));
                    assertEquals(1, values.docValueCount());
                    assertEquals(new BytesRef(""), values.nextValue());
                }
            }
        }
    }

    public void testForKeyIsNotChangedByLaterChangesToTheKey() throws IOException {
        assumeTrue("only the array order layout is read by key", layout == Layout.ARRAY_ORDER_INLINE_NULL);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                addArrayOrderDoc(writer, new Slot("k1", "one"), new Slot("k2", "two"));

                try (IndexReader reader = openReader(writer)) {
                    LeafReader leaf = reader.leaves().get(0).reader();
                    byte[] key = "k1".getBytes(StandardCharsets.UTF_8);
                    SortedBinaryDocValues values = KeyFilteredSortingArrayOrderBinaryDocValues.forKey(
                        leaf.getBinaryDocValues(KEYED_FIELD),
                        leaf.getNumericDocValues(KEYED_FIELD + ".counts"),
                        new BytesRef(key)
                    );
                    key[1] = '2';

                    assertTrue(values.advanceExact(0));
                    assertEquals(1, values.docValueCount());
                    assertEquals(new BytesRef("one"), values.nextValue());
                }
            }
        }
    }

    public void testValuesAreOrderedByTheirUtf8Bytes() throws IOException {
        List<String> values = List.of("a", "\u00e9", "\uff5e", "\ud83d\ude00", "\u0000b");
        TreeSet<BytesRef> expected = new TreeSet<>();
        for (String value : values) {
            expected.add(new BytesRef(value));
        }
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                addDoc(
                    writer,
                    layout,
                    "k1\0" + values.get(3),
                    "k2\0" + values.get(1),
                    "k3\0" + values.get(4),
                    "k4\0" + values.get(0),
                    "k5\0" + values.get(2)
                );

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues dv = loadRootValues(reader, layout);
                    assertTrue(dv.advanceExact(0));
                    List<String> actual = new ArrayList<>();
                    for (int i = 0; i < dv.docValueCount(); i++) {
                        actual.add(dv.nextValue().utf8ToString());
                    }
                    assertEquals(expected.stream().map(BytesRef::utf8ToString).toList(), actual);
                }
            }
        }
    }

    public void testRandomized() throws IOException {
        boolean arrayOrder = layout == Layout.ARRAY_ORDER_INLINE_NULL;
        int docCount = randomIntBetween(1, 20);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, layout)) {
                Set<List<String>> expectedValuesPerDoc = new HashSet<>();
                int expectedWithoutValues = 0;
                for (int d = 0; d < docCount; d++) {
                    Slot[] slots = new Slot[randomIntBetween(1, 10)];
                    TreeSet<String> uniqueValues = new TreeSet<>((a, b) -> new BytesRef(a).compareTo(new BytesRef(b)));
                    for (int f = 0; f < slots.length; f++) {
                        String key = "key" + randomIntBetween(0, 100);
                        String value = arrayOrder && rarely() ? null : randomRealisticUnicodeOfLengthBetween(arrayOrder ? 0 : 1, 20);
                        slots[f] = new Slot(key, value);
                        if (value != null) {
                            uniqueValues.add(value);
                        }
                    }
                    addSlots(writer, layout, slots);
                    if (uniqueValues.isEmpty()) {
                        expectedWithoutValues++;
                    } else {
                        expectedValuesPerDoc.add(new ArrayList<>(uniqueValues));
                    }
                }

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, layout);

                    // Merge policies may reorder documents, so collect values
                    // as a set of sorted lists without assuming document order.
                    Set<List<String>> actualValuesPerDoc = new HashSet<>();
                    int actualWithoutValues = 0;
                    for (int d = 0; d < docCount; d++) {
                        if (values.advanceExact(d) == false) {
                            actualWithoutValues++;
                            continue;
                        }
                        List<String> actual = new ArrayList<>();
                        for (int i = 0; i < values.docValueCount(); i++) {
                            actual.add(values.nextValue().utf8ToString());
                        }
                        actualValuesPerDoc.add(actual);
                    }
                    assertEquals(expectedWithoutValues, actualWithoutValues);
                    assertEquals(expectedValuesPerDoc, actualValuesPerDoc);
                }
            }
        }
    }

    private static RandomIndexWriter newRandomIndexWriter(Directory dir, Layout layout) throws IOException {
        if (layout != Layout.SORTED_SET) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
            return new RandomIndexWriter(random(), dir, iwc);
        }
        return new RandomIndexWriter(random(), dir);
    }

    private static void addDoc(RandomIndexWriter writer, Layout layout, String... keyedValues) throws IOException {
        switch (layout) {
            case SORTED_SET -> {
                Document doc = new Document();
                for (String kv : keyedValues) {
                    doc.add(new SortedSetDocValuesField(KEYED_FIELD, new BytesRef(kv)));
                }
                writer.addDocument(doc);
            }
            case SEPARATE_COUNT -> {
                Document doc = new Document();
                var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                    KEYED_FIELD,
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                );
                for (String kv : keyedValues) {
                    field.add(new BytesRef(kv));
                }
                doc.add(field);
                doc.add(NumericDocValuesField.indexedField(KEYED_FIELD + ".counts", field.count()));
                writer.addDocument(doc);
            }
            case ARRAY_ORDER_INLINE_NULL -> {
                Slot[] slots = new Slot[keyedValues.length];
                for (int i = 0; i < keyedValues.length; i++) {
                    int separator = keyedValues[i].indexOf('\0');
                    slots[i] = new Slot(keyedValues[i].substring(0, separator), keyedValues[i].substring(separator + 1));
                }
                addArrayOrderDoc(writer, slots);
            }
        }
    }

    private static void addSlots(RandomIndexWriter writer, Layout layout, Slot... slots) throws IOException {
        if (layout == Layout.ARRAY_ORDER_INLINE_NULL) {
            addArrayOrderDoc(writer, slots);
            return;
        }
        String[] keyedValues = new String[slots.length];
        for (int i = 0; i < slots.length; i++) {
            keyedValues[i] = slots[i].key() + "\0" + slots[i].value();
        }
        addDoc(writer, layout, keyedValues);
    }

    private static void addArrayOrderDoc(RandomIndexWriter writer, Slot... slots) throws IOException {
        LuceneDocument doc = new LuceneDocument();
        for (Slot slot : slots) {
            if (slot.value() == null) {
                MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull.recordNull(doc, KEYED_FIELD, new BytesRef(slot.key() + "\0"));
            } else {
                MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull.recordValue(
                    doc,
                    KEYED_FIELD,
                    new BytesRef(slot.key() + "\0" + slot.value())
                );
            }
        }
        writer.addDocument(doc);
    }

    private static IndexReader openReader(RandomIndexWriter writer) throws IOException {
        writer.forceMerge(1);
        return ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer.w), new ShardId("test", "_na_", 0));
    }

    private static SortedBinaryDocValues loadRootValues(IndexReader reader, Layout layout) {
        var builder = new RootFlattenedFromKeyedFieldData.Builder(
            ROOT_FIELD,
            KEYED_FIELD,
            layout != Layout.SORTED_SET,
            layout == Layout.ARRAY_ORDER_INLINE_NULL,
            IndexVersion.current()
        );
        IndexFieldData<?> fieldData = builder.build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
        return fieldData.load(reader.leaves().get(0)).getBytesValues();
    }
}
