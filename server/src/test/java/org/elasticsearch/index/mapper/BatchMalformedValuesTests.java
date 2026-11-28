/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfColumnTransforms;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * Checks that {@link BatchMalformedValues} adds the same fields to a batch as the row path does per document. The mapper is an
 * {@code ip} field, used only to hand a {@link FieldMapper} to the helper.
 */
public class BatchMalformedValuesTests extends MapperServiceTestCase {

    private static final String FIELD = "f";

    private MapperService mapperService(Settings.Builder settings, boolean ignoreMalformed) throws IOException {
        return createMapperService(
            settings.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC).build(),
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("ignore_malformed", ignoreMalformed).endObject())
        );
    }

    private static Settings.Builder standard() {
        return Settings.builder();
    }

    /** {@code ._on_failure} is the sidecar of strict columnar indices; the others use {@code ._ignore_malformed}. */
    private static Settings.Builder columnar() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName());
    }

    /** A malformed value: a string that is not an IP, or an integral number. */
    private static Object randomMalformed() {
        return randomBoolean()
            ? "x" + randomAlphaOfLengthBetween(1, 8)
            : (Object) randomLongBetween(-1_000_000_000_000L, 1_000_000_000_000L);
    }

    private static BytesRef encode(Object value) {
        return value instanceof Long l
            ? XContentDataHelper.encodeIntegral(l)
            : XContentDataHelper.encodeString(new BytesRef(value.toString()));
    }

    private static String json(List<Object> values) throws IOException {
        XContentBuilder b = JsonXContent.contentBuilder().startObject();
        if (values.isEmpty() == false) {
            b.field(FIELD, values);
        }
        return Strings.toString(b.endObject());
    }

    /** The fields carrying malformed-value state ({@code _ignored} and the sidecar columns), as comparable text. */
    private static List<String> relevant(List<IndexableField> fields) {
        List<String> described = new ArrayList<>();
        for (IndexableField field : fields) {
            if (field.name().equals(IgnoredFieldMapper.NAME) || field.name().startsWith(FIELD + ".")) {
                final Object value;
                if (field.name().endsWith(".counts")) {
                    // The row path exposes a count as a number; the batch column holds the same long as sortable bytes.
                    final BytesRef bytes = field.binaryValue();
                    value = bytes != null ? NumericUtils.sortableBytesToLong(bytes.bytes, bytes.offset) : field.numericValue().longValue();
                } else {
                    value = field.binaryValue() != null ? field.binaryValue()
                        : field.numericValue() != null ? field.numericValue()
                        : field.stringValue();
                }
                described.add(field.name() + " = " + value);
            }
        }
        Collections.sort(described);
        return described;
    }

    private List<List<String>> expectedOnRowPath(MapperService mapperService, List<List<Object>> docs) throws IOException {
        List<List<String>> expected = new ArrayList<>();
        for (List<Object> values : docs) {
            expected.add(relevant(mapperService.documentMapper().parse(source(json(values))).rootDoc().getFields()));
        }
        return expected;
    }

    private BatchMappingContext newContext(MapperService mapperService, List<List<Object>> docs) throws IOException {
        IndexRequest[] requests = new IndexRequest[docs.size()];
        for (int i = 0; i < requests.length; i++) {
            requests[i] = new IndexRequest("test-index").id("d" + i).source(json(docs.get(i)), XContentType.JSON);
        }
        return new BatchMappingContext(
            EngineTestCase.initFromRequests(requests),
            mapperService.mappingLookup(),
            mapperService.getIndexSettings(),
            new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY))
        );
    }

    /** What the batch holds per document once the helper has emitted, with {@code _ignored} drained as the engine does. */
    private List<List<String>> batchFields(MapperService mapperService, BatchMappingContext ctx, int docCount) throws IOException {
        mapperService.mappingLookup().getMapping().getMetadataMapperByClass(IgnoredFieldMapper.class).postColumnarParse(ctx);
        List<List<String>> actual = new ArrayList<>();
        MappedColumns.RowCursor cursor = ctx.rowCursor(c -> true);
        for (int doc = 0; doc < docCount; doc++) {
            cursor.advance();
            actual.add(relevant(cursor.fields()));
        }
        return actual;
    }

    /** Records each document's values as encoded bytes, as a mapper that encodes them itself would, and returns what the batch holds. */
    private List<List<String>> actualOnBatchPath(MapperService mapperService, boolean store, List<List<Object>> docs) throws IOException {
        try (BatchMappingContext ctx = newContext(mapperService, docs)) {
            try (BatchMalformedValues malformed = ctx.malformedValues(fieldMapper(mapperService), store)) {
                for (int doc = 0; doc < docs.size(); doc++) {
                    for (Object value : docs.get(doc)) {
                        malformed.record(doc, encode(value));
                    }
                }
                malformed.emit();
            }
            return batchFields(mapperService, ctx, docs.size());
        }
    }

    private static FieldMapper fieldMapper(MapperService mapperService) {
        return (FieldMapper) mapperService.mappingLookup().getMapper(FIELD);
    }

    private static List<List<Object>> randomDocs() {
        List<List<Object>> docs = new ArrayList<>();
        for (int d = between(1, 20); d > 0; d--) {
            List<Object> values = new ArrayList<>();
            for (int v = between(0, 3); v > 0; v--) {
                values.add(randomMalformed());
            }
            docs.add(values);
        }
        // Always one document whose values are not in sorted order, so sorting and keeping the order differ, and one long value, so the
        // length prefix takes more than one byte.
        docs.add(randomIntBetween(0, docs.size()), new ArrayList<>(List.of("xzz", "xaa", "x" + randomAlphaOfLength(between(130, 300)))));
        return docs;
    }

    /** A standard index stores a document's values sorted, in {@code ._ignore_malformed}. */
    public void testMatchesRowPathInIgnoreMalformedColumn() throws IOException {
        MapperService mapperService = mapperService(standard(), true);
        assertFalse(FallbackPostMapper.malformedUsesOnFailureColumn(mapperService.getIndexSettings()));
        List<List<Object>> docs = randomDocs();
        assertEquals(expectedOnRowPath(mapperService, docs), actualOnBatchPath(mapperService, true, docs));
    }

    /** In {@code ._on_failure} of a strict columnar index the values keep the order they were written in. */
    public void testMatchesRowPathInOnFailureColumn() throws IOException {
        MapperService mapperService = mapperService(columnar(), true);
        assertTrue(FallbackPostMapper.malformedUsesOnFailureColumn(mapperService.getIndexSettings()));
        List<List<Object>> docs = randomDocs();
        List<List<String>> expected = expectedOnRowPath(mapperService, docs);
        assertTrue(expected.toString(), expected.stream().flatMap(List::stream).anyMatch(f -> f.startsWith(FIELD + "._on_failure = ")));
        assertEquals(expected, actualOnBatchPath(mapperService, true, docs));
    }

    /** With {@code storeMalformed} false nothing is stored, but the field is still reported in {@code _ignored}. */
    public void testNotStoringStillReportsIgnored() throws IOException {
        MapperService mapperService = mapperService(standard(), true);
        List<List<Object>> docs = randomDocs();
        List<List<String>> actual = actualOnBatchPath(mapperService, false, docs);
        for (int doc = 0; doc < docs.size(); doc++) {
            final List<String> fields = actual.get(doc);
            assertEquals(docs.get(doc).isEmpty(), fields.stream().noneMatch(f -> f.startsWith(IgnoredFieldMapper.NAME)));
            assertTrue(fields.toString(), fields.stream().noneMatch(f -> f.startsWith(FIELD + ".")));
        }
    }

    /** The helper keeps its own copy of a value passed as encoded bytes, so the caller can reuse its buffer. */
    public void testRecordedBytesMayBeReused() throws IOException {
        MapperService mapperService = mapperService(standard(), true);
        List<List<Object>> docs = List.of(List.of("aaa", "bbb"));
        BytesRef first = XContentDataHelper.encodeString(new BytesRef("aaa"));
        BytesRef second = XContentDataHelper.encodeString(new BytesRef("bbb"));
        byte[] buffer = new byte[first.length];
        try (BatchMappingContext ctx = newContext(mapperService, docs)) {
            try (BatchMalformedValues malformed = ctx.malformedValues(fieldMapper(mapperService), true)) {
                System.arraycopy(first.bytes, first.offset, buffer, 0, first.length);
                malformed.record(0, new BytesRef(buffer));
                System.arraycopy(second.bytes, second.offset, buffer, 0, second.length);
                malformed.record(0, new BytesRef(buffer));
                malformed.emit();
            }
            assertEquals(expectedOnRowPath(mapperService, docs), batchFields(mapperService, ctx, 1));
        }
    }

    /** Values longer than 127 and 16383 bytes need two and three byte length prefixes, in both sidecar columns. */
    public void testLongValues() throws IOException {
        for (Settings.Builder settings : List.of(standard(), columnar())) {
            MapperService mapperService = mapperService(settings, true);
            List<List<Object>> docs = List.of(
                List.of("x" + "b".repeat(200), "x" + "a".repeat(20_000), "xc"),
                List.of("x" + "a".repeat(130))
            );
            assertEquals(expectedOnRowPath(mapperService, docs), actualOnBatchPath(mapperService, true, docs));
        }
    }

    /** The values a mapper reads from a cursor are encoded as the row path encodes the token that was written. */
    public void testCursorOverloadMatchesRowPath() throws IOException {
        for (Settings.Builder settings : List.of(standard(), columnar())) {
            MapperService mapperService = mapperService(settings, true);
            List<List<Object>> docs = List.of(
                List.of("xabc"),
                List.of(7L),
                List.of(1L << 40),
                List.of(2.5d),
                List.of(true),
                List.of(false)
            );
            BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
            try (
                EscfBatch batch = singleFieldBatch(recycler, docs);
                BatchMappingContext ctx = newContext(mapperService, docs);
                BatchMalformedValues malformed = ctx.malformedValues(fieldMapper(mapperService), true)
            ) {
                EscfColumnTransforms.TypedUtf8Cursor cursor = EscfColumnTransforms.typedUtf8Cursor(batch.column(0), false);
                for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
                    malformed.record(doc, cursor);
                }
                malformed.emit();
                assertEquals(expectedOnRowPath(mapperService, docs), batchFields(mapperService, ctx, docs.size()));
            }
        }
    }

    /** A null is never malformed, so a caller that passes one on gets a fallback, not a stored value. */
    public void testCursorOverloadRejectsNull() throws IOException {
        MapperService mapperService = mapperService(standard(), true);
        List<List<Object>> docs = List.of(List.of());
        BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (
            EscfBatch batch = nullBatch(recycler);
            BatchMappingContext ctx = newContext(mapperService, docs);
            BatchMalformedValues malformed = ctx.malformedValues(fieldMapper(mapperService), true)
        ) {
            EscfColumnTransforms.TypedUtf8Cursor cursor = EscfColumnTransforms.typedUtf8Cursor(batch.column(0), false);
            assertEquals(0, cursor.nextDoc());
            UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> malformed.record(0, cursor));
            assertThat(e.getMessage(), containsString("NULL"));
            assertThat(e.getMessage(), containsString("field [" + FIELD + "]"));
        }
    }

    public void testEmitWithoutRecordsAddsNothing() throws IOException {
        MapperService mapperService = mapperService(standard(), true);
        List<List<Object>> docs = List.of(List.of(), List.of());
        try (BatchMappingContext ctx = newContext(mapperService, docs)) {
            try (BatchMalformedValues malformed = ctx.malformedValues(fieldMapper(mapperService), true)) {
                malformed.emit();
            }
            assertEquals(List.of(List.of(), List.of()), batchFields(mapperService, ctx, docs.size()));
        }
    }

    /** Closing without emitting releases the buffered state and leaves the batch without sidecar columns. */
    public void testCloseWithoutEmitAddsNothing() throws IOException {
        MapperService mapperService = mapperService(standard(), true);
        List<List<Object>> docs = List.of(List.of("xa", "xb"), List.of("xc"));
        try (BatchMappingContext ctx = newContext(mapperService, docs)) {
            try (BatchMalformedValues malformed = ctx.malformedValues(fieldMapper(mapperService), true)) {
                malformed.record(0, encode("xa"));
                malformed.record(0, encode("xb"));
                malformed.record(1, encode("xc"));
            }
            for (List<String> fields : batchFields(mapperService, ctx, docs.size())) {
                assertTrue(fields.toString(), fields.stream().noneMatch(f -> f.startsWith(FIELD + ".")));
            }
        }
    }

    public void testMisuseTripsAssertions() throws IOException {
        MapperService mapperService = mapperService(standard(), true);
        List<List<Object>> docs = List.of(List.of(), List.of());
        try (BatchMappingContext ctx = newContext(mapperService, docs)) {
            try (BatchMalformedValues malformed = ctx.malformedValues(fieldMapper(mapperService), true)) {
                malformed.record(1, encode("xa"));
                expectThrows(AssertionError.class, () -> malformed.record(0, encode("xb")));
                malformed.emit();
                expectThrows(AssertionError.class, () -> malformed.emit());
                expectThrows(AssertionError.class, () -> malformed.record(1, encode("xc")));
            }
        }
    }

    private static EscfBatch singleFieldBatch(BytesRefRecycler recycler, List<List<Object>> docs) {
        try (EscfBatchBuilder builder = new EscfBatchBuilder(recycler)) {
            for (List<Object> values : docs) {
                EscfRowBuffer row = builder.beginRow();
                Object value = values.get(0);
                if (value instanceof String s) {
                    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                    row.stringField(FIELD, bytes, 0, bytes.length);
                } else if (value instanceof Long l) {
                    row.longField(FIELD, l);
                } else if (value instanceof Double d) {
                    row.doubleField(FIELD, d);
                } else {
                    row.booleanField(FIELD, (Boolean) value);
                }
                row.finishRow();
                builder.commit(0);
            }
            return builder.buildPartition(0);
        }
    }

    private static EscfBatch nullBatch(BytesRefRecycler recycler) {
        try (EscfBatchBuilder builder = new EscfBatchBuilder(recycler)) {
            EscfRowBuffer row = builder.beginRow();
            row.nullField(FIELD);
            row.finishRow();
            builder.commit(0);
            return builder.buildPartition(0);
        }
    }

    public void testFallsBackWhenIgnoreMalformedIsFalse() throws IOException {
        MapperService mapperService = mapperService(standard(), false);
        try (
            BatchMappingContext ctx = newContext(mapperService, List.of(List.of()));
            BatchMalformedValues malformed = ctx.malformedValues(fieldMapper(mapperService), true)
        ) {
            assertFalse(malformed.handled());
            UnsupportedOperationException e = expectThrows(
                UnsupportedOperationException.class,
                () -> malformed.record(0, encode("not-an-ip"))
            );
            assertThat(e.getMessage(), containsString("ignore_malformed is false"));
        }
    }

    public void testFallsBackWhenLayoutCannotStore() throws IOException {
        assumeTrue(
            "no write-compatible version before the SeparateCount layout",
            IndexVersionUtils.getLowestWriteCompatibleVersion().before(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES)
        );
        IndexVersion old = IndexVersionUtils.randomVersionBetween(
            IndexVersionUtils.getLowestWriteCompatibleVersion(),
            IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES)
        );
        MapperService mapperService = createMapperService(
            old,
            Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC).build(),
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("ignore_malformed", true).endObject())
        );
        try (BatchMappingContext ctx = newContext(mapperService, List.of(List.of()))) {
            try (BatchMalformedValues stores = ctx.malformedValues(fieldMapper(mapperService), true)) {
                assertFalse(stores.handled());
                UnsupportedOperationException e = expectThrows(
                    UnsupportedOperationException.class,
                    () -> stores.record(0, encode("not-an-ip"))
                );
                assertThat(e.getMessage(), containsString("index version cannot store"));
            }
            // When the row path stores nothing, the layout does not matter.
            try (BatchMalformedValues ignoresOnly = ctx.malformedValues(fieldMapper(mapperService), false)) {
                assertTrue(ignoresOnly.handled());
            }
        }
    }

    /** {@code layoutSupported} checks only the {@code SeparateCount} layout; the other two version gates are on either side of it. */
    public void testLayoutGateOrdering() {
        assertTrue(
            IndexVersions.STORE_IGNORED_MALFORMED_IN_BINARY_DOC_VALUES.onOrBefore(
                IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES
            )
        );
        assertTrue(
            IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES.onOrBefore(IndexVersions.MALFORMED_VALUES_IN_ON_FAILURE_COLUMN)
        );
    }
}
