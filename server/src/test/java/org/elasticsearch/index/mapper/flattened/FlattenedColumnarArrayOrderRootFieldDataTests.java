/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class FlattenedColumnarArrayOrderRootFieldDataTests extends AbstractFlattenedColumnarArrayOrderTestCase {

    private static final List<String> KEYS = List.of("pilot", "copilot", "droid", "passengers", "cargo");

    private record Case(CheckedConsumer<XContentBuilder, IOException> document, TreeSet<BytesRef> expected) {}

    private static IndexFieldData<?> rootFieldData(MapperService mapperService) {
        return mapperService.fieldType(FIELD)
            .fielddataBuilder(FieldDataContext.noRuntimeFields("test", "test"))
            .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
    }

    private static LeafFieldData loadDirect(IndexFieldData<?> fieldData, LeafReaderContext leaf) throws IOException {
        try {
            return fieldData.loadDirect(leaf);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private static List<String> readValues(SortableBinaryDocValues dv, int doc) throws IOException {
        final List<String> values = new ArrayList<>();
        if (dv.advanceExact(doc)) {
            final int count = dv.docValueCount();
            for (int i = 0; i < count; i++) {
                values.add(dv.nextValue().utf8ToString());
            }
        }
        return values;
    }

    private static List<String> asStrings(TreeSet<BytesRef> expected) {
        return expected.stream().map(BytesRef::utf8ToString).toList();
    }

    private List<String> rootValues(IndexMode mode, LeafArrays leafArrays, CheckedConsumer<XContentBuilder, IOException> doc)
        throws IOException {
        final MapperService mapperService = flattenedMapperService(mode, leafArrays);
        assertTrue(mapperService.documentMapper().mappers().getMapper(FIELD).storesArrayValuesInOrder());
        final List<List<String>> values = new ArrayList<>();
        withIndexedDocument(
            mapperService,
            doc,
            indexed -> values.add(readValues(rootFieldData(mapperService).load(indexed.leaf()).getBytesValues(), 0))
        );
        return values.get(0).isEmpty() ? null : values.get(0);
    }

    private List<String> rootValues(CheckedConsumer<XContentBuilder, IOException> doc) throws Exception {
        final Map<String, List<String>> byVariant = new LinkedHashMap<>();
        forEachStrictColumnarModeAndLeafArrays(
            (mode, leafArrays) -> byVariant.put(mode.getName() + "/" + leafArrays, rootValues(mode, leafArrays, doc))
        );
        final List<String> first = byVariant.values().iterator().next();
        byVariant.forEach((variant, values) -> assertEquals("variant=" + variant, first, values));
        return first;
    }

    private static Case keyedCase(Map<String, Object> keyedValues) {
        final TreeSet<BytesRef> expected = new TreeSet<>();
        for (final Object value : keyedValues.values()) {
            if (value instanceof String string) {
                expected.add(new BytesRef(string));
            } else {
                for (final Object element : (List<?>) value) {
                    if (element != null) {
                        expected.add(new BytesRef((String) element));
                    }
                }
            }
        }
        return new Case(b -> {
            b.startObject(FIELD);
            for (final Map.Entry<String, Object> entry : keyedValues.entrySet()) {
                if (entry.getValue() instanceof String value) {
                    b.field(entry.getKey(), value);
                } else {
                    b.startArray(entry.getKey());
                    for (final Object element : (List<?>) entry.getValue()) {
                        if (element == null) {
                            b.nullValue();
                        } else {
                            b.value((String) element);
                        }
                    }
                    b.endArray();
                }
            }
            b.endObject();
        }, expected);
    }

    private static Case noValuesCase(CheckedConsumer<XContentBuilder, IOException> document) {
        return new Case(document, new TreeSet<>());
    }

    public void testTwoKeysWithOneValueEach() throws Exception {
        assertEquals(
            List.of("chewbacca", "han solo"),
            rootValues(b -> b.startObject(FIELD).field("pilot", "han solo").field("copilot", "chewbacca").endObject())
        );
    }

    public void testOneKeyWithTwoValues() throws Exception {
        assertEquals(
            List.of("luke skywalker", "princess leia"),
            rootValues(
                b -> b.startObject(FIELD).startArray("passengers").value("princess leia").value("luke skywalker").endArray().endObject()
            )
        );
    }

    public void testNullElementsAndEmptyStringsWrittenByTheMapper() throws Exception {
        assertEquals(
            "null elements are dropped",
            List.of("chewbacca", "han solo"),
            rootValues(
                b -> b.startObject(FIELD)
                    .startArray("passengers")
                    .nullValue()
                    .value("han solo")
                    .nullValue()
                    .endArray()
                    .field("copilot", "chewbacca")
                    .endObject()
            )
        );
        assertNull(
            "arrays of only nulls have no value",
            rootValues(
                b -> b.startObject(FIELD)
                    .startArray("passengers")
                    .nullValue()
                    .nullValue()
                    .endArray()
                    .startArray("cargo")
                    .nullValue()
                    .endArray()
                    .endObject()
            )
        );
        assertNull("a lone null slot has no value", rootValues(b -> b.startObject(FIELD).nullField("passengers").endObject()));
        assertEquals(
            "an empty string is a value and a null is not",
            List.of(""),
            rootValues(
                b -> b.startObject(FIELD).startArray("passengers").nullValue().value("").nullValue().value("").endArray().endObject()
            )
        );
    }

    public void testDocumentsWithoutAnyValueHaveNoValues() throws Exception {
        assertNull(rootValues(b -> {}));
        assertNull(rootValues(b -> b.nullField(FIELD)));
        assertNull(rootValues(b -> b.startObject(FIELD).endObject()));
        assertNull(rootValues(b -> b.startObject(FIELD).startArray("passengers").endArray().endObject()));
    }

    public void testNestedKeysContributeTheirValues() throws Exception {
        assertEquals(
            List.of("chewbacca", "han solo", "r2-d2"),
            rootValues(
                b -> b.startObject(FIELD)
                    .startObject("ship")
                    .field("pilot", "han solo")
                    .startArray("crew")
                    .value("chewbacca")
                    .value("r2-d2")
                    .endArray()
                    .endObject()
                    .endObject()
            )
        );
    }

    public void testValueLengthsAroundTheVIntBoundaries() throws Exception {
        for (final int length : new int[] { 126, 127, 128, 16382, 16383, 16384 }) {
            final String first = "a".repeat(length);
            final String second = "b".repeat(length);
            assertEquals(
                "length " + length,
                List.of(first, second),
                rootValues(b -> b.startObject(FIELD).field("pilot", second).field("copilot", first).endObject())
            );
        }
    }

    public void testReadersKeepIndependentStateAcrossDocuments() throws Exception {
        final List<Case> corpus = new ArrayList<>();
        corpus.add(noValuesCase(b -> {}));
        corpus.add(noValuesCase(b -> b.nullField(FIELD)));
        corpus.add(noValuesCase(b -> b.startObject(FIELD).endObject()));
        corpus.add(keyedCase(Map.of("passengers", Arrays.asList(null, null))));
        corpus.add(keyedCase(Map.of("pilot", "han solo")));
        corpus.add(
            keyedCase(Map.of("pilot", List.of("x", "x", "y", "y", "x"), "copilot", "x", "cargo", List.of("y"), "droid", List.of("", "x")))
        );
        final Map<String, Object> distinct = new LinkedHashMap<>();
        for (final String key : KEYS) {
            final List<String> elements = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                elements.add(key + i);
            }
            distinct.put(key, elements);
        }
        corpus.add(keyedCase(distinct));
        Collections.shuffle(corpus, random());

        final List<CheckedConsumer<XContentBuilder, IOException>> documents = corpus.stream().map(Case::document).toList();
        for (final boolean direct : new boolean[] { false, true }) {
            forEachStrictColumnarModeAndLeafArrays((mode, leafArrays) -> {
                final MapperService mapperService = flattenedMapperService(mode, leafArrays);
                withIndexedDocuments(mapperService, documents, indexed -> {
                    final IndexFieldData<?> fieldData = rootFieldData(mapperService);
                    final LeafFieldData leafData = direct ? loadDirect(fieldData, indexed.leaf()) : fieldData.load(indexed.leaf());
                    assertReadersKeepIndependentState(direct, corpus, indexed, leafData);
                });
            });
        }
    }

    private static void assertReadersKeepIndependentState(boolean direct, List<Case> corpus, IndexedLeaf indexed, LeafFieldData leafData)
        throws IOException {
        final SortableBinaryDocValues reader = leafData.getBytesValues();
        final SortableBinaryDocValues laggingReader = leafData.getBytesValues();
        final DocValuesScriptFieldFactory script = leafData.getScriptFieldFactory(FIELD);
        final int maxDoc = indexed.leaf().reader().maxDoc();
        assertEquals(corpus.size(), maxDoc);
        final String message = "direct=" + direct + " doc ";
        for (int doc = 0; doc < maxDoc; doc++) {
            final List<String> expected = asStrings(corpus.get(indexed.sequenceByDoc()[doc]).expected());
            assertEquals(message + doc, expected, readValues(reader, doc));
            assertEquals(message + doc + " repositioned", expected, readValues(reader, doc));

            script.setNextDocId(doc);
            final List<String> scriptValues = new ArrayList<>();
            for (int i = 0; i < script.toScriptDocValues().size(); i++) {
                scriptValues.add(script.toScriptDocValues().get(i).toString());
            }
            assertEquals(message + doc + " script", expected, scriptValues);

            if (doc > 0) {
                final List<String> previous = asStrings(corpus.get(indexed.sequenceByDoc()[doc - 1]).expected());
                assertEquals(message + (doc - 1) + " lagging reader", previous, readValues(laggingReader, doc - 1));
            }
        }
    }
}
