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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Raw binary columns are mapped in batch to the same fields as the row path, which reads the bytes from base64 in JSON.
 */
public class BinaryFieldMapperColumnarBatchTests extends MapperServiceTestCase {

    private static final String FIELD = "f";

    private static Settings columnar(SourceFieldMapper.Mode sourceMode) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), sourceMode.toString())
            .build();
    }

    private MapperService mapperService(SourceFieldMapper.Mode sourceMode, boolean multiValue) throws IOException {
        return createMapperService(
            columnar(sourceMode),
            mapping(b -> b.startObject(FIELD).field("type", "binary").field("multi_value", multiValue).endObject())
        );
    }

    private static FieldMapper fieldMapper(MapperService mapperService) {
        return (FieldMapper) mapperService.mappingLookup().getMapper(FIELD);
    }

    public void testOnlySingleValuedIsSupported() throws IOException {
        MapperService singleValued = mapperService(SourceFieldMapper.Mode.SYNTHETIC, false);
        assertTrue(fieldMapper(singleValued).supportsColumnarParse(singleValued.getIndexSettings()));
        MapperService multiValued = mapperService(SourceFieldMapper.Mode.SYNTHETIC, true);
        assertFalse(fieldMapper(multiValued).supportsColumnarParse(multiValued.getIndexSettings()));
    }

    public void testRawBinaryMatchesRowPath() throws IOException {
        assertRawBinaryMatchesRowPath(SourceFieldMapper.Mode.SYNTHETIC);
    }

    /** Without a synthetic source license, columnar indices store the source, but binary values stay out of it. */
    public void testRawBinaryMatchesRowPathWithStoredSource() throws IOException {
        assertRawBinaryMatchesRowPath(SourceFieldMapper.Mode.COLUMNAR_STORED);
    }

    /** A JSON source carries base64 strings, so its batches take the row path from the start. */
    public void testBase64TakesRowPath() throws IOException {
        MapperService mapperService = mapperService(SourceFieldMapper.Mode.SYNTHETIC, false);
        try (EscfEncoder encoder = new EscfEncoder(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            encoder.addDocument(new BytesArray("{\"f\":\"AQID\"}"), XContentType.JSON, 0);
            try (EscfBatch batch = encoder.buildPartition(0)) {
                assertNull(ShardBatchMapper.resolveMappers(batch, mapperService.mappingLookup(), mapperService.getIndexSettings()));
            }
        }
        BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (EscfBatch batch = rawBatch(List.of(new BytesRef(new byte[] { 1, 2, 3 })), recycler)) {
            assertNotNull(ShardBatchMapper.resolveMappers(batch, mapperService.mappingLookup(), mapperService.getIndexSettings()));
        }
    }

    private void assertRawBinaryMatchesRowPath(SourceFieldMapper.Mode sourceMode) throws IOException {
        MapperService mapperService = mapperService(sourceMode, false);
        assertTrue(fieldMapper(mapperService).supportsColumnarParse(mapperService.getIndexSettings()));
        int docCount = between(1, 20);
        List<BytesRef> values = new ArrayList<>(docCount);
        IndexRequest[] requests = new IndexRequest[docCount];
        for (int i = 0; i < docCount; i++) {
            // the first document keeps a value so that the column exists
            values.add(i > 0 && rarely() ? null : new BytesRef(randomByteArrayOfLength(between(0, 512))));
            requests[i] = new IndexRequest("test-index").id("d" + i).source(json(values.get(i)));
        }
        BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (
            BatchMappingContext ctx = new BatchMappingContext(
                EngineTestCase.initFromRequests(requests),
                mapperService.mappingLookup(),
                mapperService.getIndexSettings(),
                recycler
            );
            EscfBatch batch = rawBatch(values, recycler)
        ) {
            assertEquals(EscfColumnKind.BINARY, batch.column(0).kind());
            fieldMapper(mapperService).mapColumnBatch(ctx, batch.column(0));

            MappedColumns.RowCursor cursor = ctx.rowCursor(c -> true);
            for (int i = 0; i < docCount; i++) {
                cursor.advance();
                List<IndexableField> actual = cursor.fields().stream().filter(f -> f.name().equals(FIELD)).toList();
                String json = Strings.toString(json(values.get(i)));
                List<IndexableField> expected = mapperService.documentMapper().parse(source(json)).rootDoc().getFields(FIELD);
                assertEquals("doc [" + i + "]", expected.size(), actual.size());
                for (int f = 0; f < expected.size(); f++) {
                    assertEquals(expected.get(f).fieldType().docValuesType(), actual.get(f).fieldType().docValuesType());
                    assertEquals("doc [" + i + "]", expected.get(f).binaryValue(), actual.get(f).binaryValue());
                }
            }
        }
    }

    private static XContentBuilder json(BytesRef value) throws IOException {
        XContentBuilder b = JsonXContent.contentBuilder().startObject();
        if (value != null) {
            b.field(FIELD, BytesRef.deepCopyOf(value).bytes);
        }
        return b.endObject();
    }

    private static EscfBatch rawBatch(List<BytesRef> values, BytesRefRecycler recycler) {
        try (EscfBatchBuilder builder = new EscfBatchBuilder(recycler)) {
            for (BytesRef value : values) {
                EscfRowBuffer row = builder.beginRow();
                if (value != null) {
                    row.binaryField(FIELD, value);
                }
                row.finishRow();
                builder.commit(0);
            }
            return builder.buildPartition(0);
        }
    }
}
