/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.PerFieldFormatSupplier;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * A {@code binary} field in a strictly columnar index: written as a ColumNAR payload column, or as the value's own bytes under
 * {@code multi_value: false}, and left out of the {@code columnar_stored} source blob to be patched back from doc values.
 */
public class BinaryFieldMapperColumnarTests extends MapperServiceTestCase {

    private static final String A = base64("first");
    private static final String B = base64("second");

    public void testWrittenAsAPayloadColumn() throws IOException {
        final MapperService mapperService = createMapperService(settings(randomSourceMode()), binaryMapping(null));
        final BinaryFieldMapper mapper = (BinaryFieldMapper) mapperService.mappingLookup().getMapper("bin");
        assertTrue(mapper.fieldType().hasDocValues());
        assertThat(mapper.fieldType().columnarFormat(), equalTo(BinaryDocValuesFormat.COLUMNAR_PAYLOAD));
        assertThat(mapper.columnarStringOptions(), equalTo(StringColumnOptions.PAYLOADS));

        final PerFieldFormatSupplier formats = new PerFieldFormatSupplier(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
        assertThat(formats.getDocValuesFormatForField("bin"), instanceOf(ColumNARDocValuesFormat.class));

        final ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("bin", A, B, A)));
        assertThat(doc.rootDoc().getField("bin"), instanceOf(ColumnarBinaryDocValuesField.class));
    }

    public void testSingleValuedIsWrittenAsItsOwnBytes() throws IOException {
        final MapperService mapperService = createMapperService(settings(randomSourceMode()), binaryMapping(false));
        final BinaryFieldMapper mapper = (BinaryFieldMapper) mapperService.mappingLookup().getMapper("bin");
        assertThat(mapper.fieldType().columnarFormat(), equalTo(BinaryDocValuesFormat.PLAIN));
        assertThat(mapper.columnarStringOptions(), equalTo(StringColumnOptions.PAYLOADS.withSingleValued(true)));

        final ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("bin", A)));
        final IndexableField field = doc.rootDoc().getField("bin");
        assertThat(field.binaryValue(), equalTo(new BytesRef(Base64.getDecoder().decode(A))));

        final Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapperService.documentMapper().parse(source(b -> b.array("bin", A, B)))
        );
        assertThat(e.getCause().getMessage(), containsString("multi_value=false"));
    }

    /**
     * The codec off, a columnar index keeps the layout binary fields always had, and {@code columnar_stored} patches nothing in. Every
     * columnar index created from {@link IndexVersions#COLUMNAR_CODEC_NO_FEATURE_FLAG} uses the codec, so this one is created just before.
     */
    public void testWithoutTheCodecKeepsTheCustomLayout() throws IOException {
        final MapperService mapperService = createMapperService(
            IndexVersionUtils.getPreviousVersion(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG),
            settings(SourceFieldMapper.Mode.COLUMNAR_STORED),
            binaryMapping(null)
        );
        final BinaryFieldMapper mapper = (BinaryFieldMapper) mapperService.mappingLookup().getMapper("bin");
        assertThat(mapper.fieldType().columnarFormat(), nullValue());
        assertThat(mapper.columnarStringOptions(), nullValue());
        assertThat("the custom layout sorts its values, so nothing is patched", mapper.syntheticVectorsLoader(), nullValue());
        final ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("bin", A)));
        assertThat(doc.rootDoc().getField("bin"), instanceOf(BinaryFieldMapper.CustomBinaryDocValuesField.class));
    }

    /** Array order and duplicates survive, under both source modes a columnar index takes. */
    public void testSourceKeepsArrayOrderAndDuplicates() throws IOException {
        for (SourceFieldMapper.Mode mode : List.of(SourceFieldMapper.Mode.SYNTHETIC, SourceFieldMapper.Mode.COLUMNAR_STORED)) {
            final MapperService mapperService = createMapperService(settings(mode), binaryMapping(null));
            assertSource(mapperService, b -> b.array("bin", B, A, B).field("kwd", "k"), Map.of("bin", List.of(B, A, B), "kwd", "k"));
            assertSource(mapperService, b -> b.field("bin", A), Map.of("bin", A));
        }
    }

    public void testSingleValuedSourceRoundTrips() throws IOException {
        for (SourceFieldMapper.Mode mode : List.of(SourceFieldMapper.Mode.SYNTHETIC, SourceFieldMapper.Mode.COLUMNAR_STORED)) {
            final MapperService mapperService = createMapperService(settings(mode), binaryMapping(false));
            assertSource(mapperService, b -> b.field("bin", B).field("kwd", "k"), Map.of("bin", B, "kwd", "k"));
        }
    }

    /**
     * Under {@code on_failure: ignore} a second value is set aside, and both source modes still return every value: synthetic source
     * from the failure column, {@code columnar_stored} from the source blob, which keeps the field rather than patching it.
     */
    public void testSingleValuedIgnoresASecondValue() throws IOException {
        for (SourceFieldMapper.Mode mode : List.of(SourceFieldMapper.Mode.SYNTHETIC, SourceFieldMapper.Mode.COLUMNAR_STORED)) {
            final MapperService mapperService = createMapperService(settings(mode), mapping(b -> {
                b.startObject("bin").field("type", "binary");
                b.field("multi_value", false).field("on_failure", "ignore");
                b.endObject();
                b.startObject("kwd").field("type", "keyword").endObject();
            }));
            assertSource(mapperService, b -> b.array("bin", A, B, A).field("kwd", "k"), Map.of("bin", List.of(A, B, A), "kwd", "k"));
            assertSource(mapperService, b -> b.field("bin", A), Map.of("bin", A));
        }
    }

    /** Under {@code columnar_stored} the values live in doc values only: the source blob leaves them out. */
    public void testColumnarStoredBlobLeavesTheValuesOut() throws IOException {
        final MapperService mapperService = createMapperService(settings(SourceFieldMapper.Mode.COLUMNAR_STORED), binaryMapping(null));
        assertTrue(mapperService.mappingLookup().syntheticVectorFields().contains("bin"));
        final ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("bin", A, B).field("kwd", "k")));
        final String blob = new BytesArray(doc.rootDoc().getField(IgnoredSourceFieldMapper.NAME).binaryValue()).utf8ToString();
        assertThat(blob.substring(blob.indexOf('{')), equalTo("{\"kwd\":\"k\"}"));
    }

    /** Synthetic source rebuilds every field from doc values, so nothing is patched in. */
    public void testSyntheticSourcePatchesNothing() throws IOException {
        final MapperService mapperService = createMapperService(settings(SourceFieldMapper.Mode.SYNTHETIC), binaryMapping(null));
        assertThat(mapperService.mappingLookup().syntheticVectorFields(), not(org.hamcrest.Matchers.hasItem("bin")));
    }

    /** Fielddata reads the values in the order they were written. */
    public void testFielddataReadsTheColumn() throws IOException {
        final MapperService mapperService = createMapperService(settings(randomSourceMode()), binaryMapping(null));
        final ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("bin", B, A)));
        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            final var fielddata = mapperService.fieldType("bin")
                .fielddataBuilder(FieldDataContext.noRuntimeFields("index", "test"))
                .build(null, null);
            final LeafReaderContext leaf = reader.leaves().get(0);
            final SortableBinaryDocValues values = fielddata.load(leaf).getBytesValues();
            assertTrue(values.advanceExact(0));
            final List<String> read = new ArrayList<>();
            for (int i = 0; i < values.docValueCount(); i++) {
                read.add(Base64.getEncoder().encodeToString(BytesRef.deepCopyOf(values.nextValue()).bytes));
            }
            assertThat(read, equalTo(List.of(B, A)));
        });
    }

    private void assertSource(
        MapperService mapperService,
        CheckedConsumer<XContentBuilder, IOException> build,
        Map<String, Object> expected
    ) throws IOException {
        final ParsedDocument doc = mapperService.documentMapper().parse(source(build));
        withLuceneIndex(
            mapperService,
            iw -> iw.addDocuments(doc.docs()),
            reader -> assertThat(sourceOf(mapperService, reader), equalTo(expected))
        );
    }

    private static Map<String, Object> sourceOf(MapperService mapperService, DirectoryReader reader) throws IOException {
        final SourceLoader loader = mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP, null);
        final LeafReaderContext leaf = reader.leaves().get(0);
        final int[] docIds = IntStream.range(0, leaf.reader().maxDoc()).toArray();
        final LeafStoredFieldLoader storedFields = StoredFieldLoader.create(false, loader.requiredStoredFields()).getLoader(leaf, docIds);
        storedFields.advanceTo(0);
        final Source source = loader.leaf(leaf, docIds).source(storedFields, 0);
        return source.source();
    }

    private static SourceFieldMapper.Mode randomSourceMode() {
        return randomFrom(SourceFieldMapper.Mode.SYNTHETIC, SourceFieldMapper.Mode.COLUMNAR_STORED);
    }

    private static Settings settings(SourceFieldMapper.Mode sourceMode) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), sourceMode.toString())
            .build();
    }

    /** {@code bin}, multi-valued unless {@code multiValue} is {@code false}, beside a keyword. */
    private static XContentBuilder binaryMapping(Boolean multiValue) throws IOException {
        return mapping(b -> {
            b.startObject("bin").field("type", "binary");
            if (multiValue != null) {
                b.field("multi_value", multiValue);
            }
            b.endObject();
            b.startObject("kwd").field("type", "keyword").endObject();
        });
    }

    private static String base64(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
