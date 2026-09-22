/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Shared round-trip coverage for high-cardinality fields in strictly columnar mode that store their values in document order with inline
 * nulls ({@link MultiValuedBinaryDocValuesField.ArrayOrderInlineNull}) instead of a sidecar {@code .offsets} field. Concrete subclasses
 * supply the field type (keyword, text, match_only_text). Exercises order preservation, duplicates, interleaved nulls, lone/all nulls,
 * empty arrays, and the empty-string-vs-null distinction.
 */
public abstract class AbstractColumnarArrayOrderSyntheticSourceTestCase extends MapperServiceTestCase {

    /**
     * The {@code type} of the field under test; the field is always named {@code field} and is mapped with its columnar-mode defaults.
     */
    protected abstract String fieldTypeName();

    /** Each of the layouts this build writes a columnar index in: the ColumNAR codec's payload, and the layouts it replaces. */
    protected List<Boolean> layouts() {
        return ColumnarCodecSettings.AVAILABLE ? List.of(false, true) : List.of(false);
    }

    protected MapperService columnarMapperService(boolean codec) throws IOException {
        final Settings settings = ColumnarCodecSettings.name(
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()),
            codec
        ).build();
        return createMapperService(settings, mapping(b -> b.startObject("field").field("type", fieldTypeName()).endObject()));
    }

    /** The layouts the ColumNAR codec replaces, for the tests here that read one of their readers back rather than a document. */
    protected MapperService columnarMapperService() throws IOException {
        return columnarMapperService(false);
    }

    protected DocumentMapper columnarMapper() throws IOException {
        return columnarMapperService().documentMapper();
    }

    /**
     * Round-trips {@code doc} through every layout this build writes and requires them to agree on {@code expected}: what a document
     * holds is the field's meaning rather than the layout's, so an array's null survives in each of them while a scalar null and an
     * empty array are an absence of value in each of them.
     */
    protected void assertSourceInEveryLayout(String expected, CheckedConsumer<XContentBuilder, IOException> doc) throws IOException {
        for (boolean codec : layouts()) {
            assertEquals("codec=" + codec, expected, syntheticSource(columnarMapperService(codec).documentMapper(), doc));
        }
    }

    public void testOffsetsFieldNotUsed() throws IOException {
        var mapper = columnarMapper();
        // The high-cardinality columnar path stores values in order and must not allocate a sidecar offsets field.
        assertNull(mapper.mappers().getMapper("field").getOffsetFieldName());
        assertTrue(mapper.mappers().getMapper("field").storesArrayValuesInOrder());
    }

    public void testOrderAndDuplicatesPreserved() throws IOException {
        assertSourceInEveryLayout("""
            {"field":["b","a","a","c"]}""", b -> b.array("field", "b", "a", "a", "c"));
    }

    public void testSingleValueCollapsesToScalar() throws IOException {
        var mapper = columnarMapper();
        assertEquals("""
            {"field":"a"}""", syntheticSource(mapper, b -> b.array("field", "a")));
        assertEquals("""
            {"field":"a"}""", syntheticSource(mapper, b -> b.field("field", "a")));
    }

    public void testInterleavedNullsPreserved() throws IOException {
        assertSourceInEveryLayout("""
            {"field":["a",null,"b"]}""", b -> b.startArray("field").value("a").nullValue().value("b").endArray());
    }

    public void testAllNullArray() throws IOException {
        assertSourceInEveryLayout("""
            {"field":[null,null]}""", b -> b.startArray("field").nullValue().nullValue().endArray());
    }

    public void testLoneNull() throws IOException {
        assertSourceInEveryLayout("""
            {"field":[null]}""", b -> b.startArray("field").nullValue().endArray());
    }

    /**
     * A scalar {@code null} (written via {@code b.nullField("field")}) holds no array element, so a strict columnar index records no
     * slot for it and the field reads back as absent — as it does outside one. A null inside an array keeps its slot, which
     * {@link #testLoneNull()} pins.
     */
    public void testScalarNullIsAbsent() throws IOException {
        assertSourceInEveryLayout("{}", b -> b.nullField("field"));
    }

    public void testEmptyArray() throws IOException {
        assertSourceInEveryLayout("{}", b -> b.startArray("field").endArray());
        var mapper = columnarMapper();
        // An empty array has no values to store in a doc-value column and isn't field-owned, so columnar drops it
        // (lossy) rather than keeping a generic _ignored_source marker. The field is therefore absent from _source.
        assertEquals("{}", syntheticSource(mapper, b -> b.startArray("field").endArray()));
    }

    public void testEmptyStringDistinctFromNull() throws IOException {
        var mapper = columnarMapper();
        assertEquals("""
            {"field":""}""", syntheticSource(mapper, b -> b.array("field", "")));
        assertEquals("""
            {"field":["a","",null,"b"]}""", syntheticSource(mapper, b -> {
            b.startArray("field").value("a").value("").nullValue().value("b").endArray();
        }));
    }

    public void testColumnarSingleValuedSelectsFastPathReader() throws IOException {
        // Every document is single-valued so the .counts skipper reports maxValue == 1; the loader must select the plain reader, which can
        // use the OptionalColumnAtATimeReader bulk fast path, rather than the inline-null decoder that always reads document-by-document.
        MapperService mapperService = columnarMapperService();
        assertTrue(mapperService.documentMapper().mappers().getMapper("field").storesArrayValuesInOrder());

        String value0 = randomAlphanumericOfLength(8);
        String value1 = randomAlphanumericOfLength(8);
        String value2 = randomAlphanumericOfLength(8);

        withLuceneIndex(mapperService, iw -> {
            DocumentMapper mapper = mapperService.documentMapper();
            iw.addDocument(mapper.parse(source(b -> b.field("field", value0))).rootDoc());
            iw.addDocument(mapper.parse(source(b -> b.field("field", value1))).rootDoc());
            iw.addDocument(mapper.parse(source(b -> b.field("field", value2))).rootDoc());
            iw.forceMerge(1);
        }, reader -> {
            try (BlockLoader.ColumnAtATimeReader columnReader = columnarColumnReader(mapperService, reader.leaves().get(0))) {
                assertThat(columnReader, instanceOf(BytesRefsFromBinaryBlockLoader.BytesRefsFromBinary.class));

                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0, 1, 2), 0, randomBoolean());
                assertThat(((BytesRef) block.get(0)).utf8ToString(), equalTo(value0));
                assertThat(((BytesRef) block.get(1)).utf8ToString(), equalTo(value1));
                assertThat(((BytesRef) block.get(2)).utf8ToString(), equalTo(value2));
            }
        });
    }

    public void testColumnarMultiValuedSelectsArrayOrderReader() throws IOException {
        // A document with two values pushes the .counts skipper maxValue to >= 2, so the loader must select the inline-null decoder; the
        // plain reader's bulk fast path would misread the multi-slot [len+1][val] encoding as a single raw value.
        MapperService mapperService = columnarMapperService();

        String value0 = randomAlphanumericOfLength(8);
        String value1 = randomAlphanumericOfLength(8);

        withLuceneIndex(mapperService, iw -> {
            DocumentMapper mapper = mapperService.documentMapper();
            iw.addDocument(mapper.parse(source(b -> b.array("field", value0, value1))).rootDoc());
            iw.forceMerge(1);
        }, reader -> {
            try (BlockLoader.ColumnAtATimeReader columnReader = columnarColumnReader(mapperService, reader.leaves().get(0))) {
                // The inline-null decoder is package-private here, so assert by exclusion that the fast-path reader was not selected.
                assertThat(columnReader, not(instanceOf(BytesRefsFromBinaryBlockLoader.BytesRefsFromBinary.class)));

                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0), 0, randomBoolean());
                // Both values come back in document (array) order.
                List<?> values = (List<?>) block.get(0);
                assertThat(values, hasSize(2));
                assertThat(((BytesRef) values.get(0)).utf8ToString(), equalTo(value0));
                assertThat(((BytesRef) values.get(1)).utf8ToString(), equalTo(value1));
            }
        });
    }

    private BlockLoader.ColumnAtATimeReader columnarColumnReader(MapperService mapperService, LeafReaderContext leaf) throws IOException {
        BlockLoader blockLoader = mapperService.fieldType("field")
            .blockLoader(new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapperService));
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        return blockLoader.columnAtATimeReader(leaf).apply(breaker);
    }
}
