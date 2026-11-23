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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests {@link BatchMalformedValues} against the row path: for the same malformed values, the fields it adds to a batch must equal the
 * ones {@link FallbackPostMapper#captureMalformed} and {@code addIgnoredField} add per document. The mapper under test is an {@code ip}
 * field with {@code ignore_malformed}, used only as a {@link FieldMapper} to hand to the helper.
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

    /** The fields that carry malformed-value state: {@code _ignored} and the field's sidecar columns, as comparable text. */
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

    /** Records each document's values through the helper, as a mapper would, and returns what the batch holds per document. */
    private List<List<String>> actualOnBatchPath(MapperService mapperService, boolean store, List<List<Object>> docs) throws IOException {
        IndexRequest[] requests = new IndexRequest[docs.size()];
        for (int i = 0; i < requests.length; i++) {
            requests[i] = new IndexRequest("test-index").id("d" + i).source(json(docs.get(i)), XContentType.JSON);
        }
        BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (
            BatchMappingContext ctx = new BatchMappingContext(
                EngineTestCase.initFromRequests(requests),
                mapperService.mappingLookup(),
                mapperService.getIndexSettings(),
                recycler
            )
        ) {
            try (BatchMalformedValues malformed = ctx.malformedValues(fieldMapper(mapperService), store)) {
                for (int doc = 0; doc < docs.size(); doc++) {
                    for (Object value : docs.get(doc)) {
                        malformed.record(doc, encode(value));
                    }
                }
                malformed.emit();
            }
            mapperService.mappingLookup().getMapping().getMetadataMapperByClass(IgnoredFieldMapper.class).postColumnarParse(ctx);
            List<List<String>> actual = new ArrayList<>();
            MappedColumns.RowCursor cursor = ctx.rowCursor(c -> true);
            for (int doc = 0; doc < docs.size(); doc++) {
                cursor.advance();
                actual.add(relevant(cursor.fields()));
            }
            return actual;
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
        // A batch with nothing malformed adds nothing, which is a different case.
        if (docs.stream().allMatch(List::isEmpty)) {
            docs.get(0).add(randomMalformed());
        }
        return docs;
    }

    /** Sorted into {@code ._ignore_malformed} on a standard index: one value is raw, several are length-prefixed and in sorted order. */
    public void testMatchesRowPathInIgnoreMalformedColumn() throws IOException {
        MapperService mapperService = mapperService(standard(), true);
        List<List<Object>> docs = randomDocs();
        assertEquals(expectedOnRowPath(mapperService, docs), actualOnBatchPath(mapperService, true, docs));
    }

    /** In {@code ._on_failure} of a strict columnar index the values keep the order they were written in. */
    public void testMatchesRowPathInOnFailureColumn() throws IOException {
        MapperService mapperService = mapperService(columnar(), true);
        List<List<Object>> docs = randomDocs();
        assertEquals(expectedOnRowPath(mapperService, docs), actualOnBatchPath(mapperService, true, docs));
    }

    /** Without synthetic source nothing is stored, but the field is still reported in {@code _ignored}. */
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
        IndexRequest[] requests = { new IndexRequest("test-index").id("d0").source(json(docs.get(0)), XContentType.JSON) };
        BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (
            BatchMappingContext ctx = new BatchMappingContext(
                EngineTestCase.initFromRequests(requests),
                mapperService.mappingLookup(),
                mapperService.getIndexSettings(),
                recycler
            );
            BatchMalformedValues malformed = ctx.malformedValues(fieldMapper(mapperService), true)
        ) {
            System.arraycopy(first.bytes, first.offset, buffer, 0, first.length);
            malformed.record(0, new BytesRef(buffer));
            System.arraycopy(second.bytes, second.offset, buffer, 0, second.length);
            malformed.record(0, new BytesRef(buffer));
            malformed.emit();
            MappedColumns.RowCursor cursor = ctx.rowCursor(c -> true);
            cursor.advance();
            assertEquals(
                expectedOnRowPath(mapperService, docs).get(0).stream().filter(f -> f.startsWith(FIELD + ".")).toList(),
                relevant(cursor.fields())
            );
        }
    }

    public void testFallsBackWhenIgnoreMalformedIsFalse() throws IOException {
        MapperService mapperService = mapperService(standard(), false);
        try (
            BatchMappingContext ctx = newContext(mapperService);
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
        IndexVersion old = IndexVersionUtils.randomVersionBetween(
            IndexVersionUtils.getLowestWriteCompatibleVersion(),
            IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES)
        );
        MapperService mapperService = createMapperService(
            old,
            Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC).build(),
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("ignore_malformed", true).endObject())
        );
        try (BatchMappingContext ctx = newContext(mapperService)) {
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

    private BatchMappingContext newContext(MapperService mapperService) {
        IndexRequest[] requests = { new IndexRequest("test-index").id("d0").source("{}", XContentType.JSON) };
        return new BatchMappingContext(
            EngineTestCase.initFromRequests(requests),
            mapperService.mappingLookup(),
            mapperService.getIndexSettings(),
            new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY))
        );
    }

    /**
     * {@code layoutSupported} checks only the {@code SeparateCount} layout, which is sufficient because the other version gates that
     * the row path consults for malformed values are not later than it.
     */
    public void testLayoutGate() {
        assertTrue(BatchMalformedValues.layoutSupported(IndexVersionUtils.randomVersion(), false));
        assertTrue(BatchMalformedValues.layoutSupported(IndexVersion.current(), true));
        assertTrue(
            BatchMalformedValues.layoutSupported(
                IndexVersionUtils.randomVersionBetween(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES, IndexVersion.current()),
                true
            )
        );
        assertFalse(
            BatchMalformedValues.layoutSupported(
                IndexVersionUtils.randomVersionBetween(
                    null,
                    IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES)
                ),
                true
            )
        );
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
