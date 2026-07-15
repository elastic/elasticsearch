/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class IgnoredSourceFormatFlipTests extends MapperServiceTestCase {

    public void testFormatFlipFailsWritePath() throws IOException {
        try (
            MapperService storedFormatMapperService = storedFormatMapperService();
            MapperService docValuesFormatMapperService = docValuesFormatMapperService()
        ) {
            final ParsedDocument storedFormatDocument = parseTimeSeriesDocument(storedFormatMapperService.documentMapper());
            final ParsedDocument docValuesFormatDocument = parseTimeSeriesDocument(docValuesFormatMapperService.documentMapper());

            try (Directory directory = newDirectory()) {
                final IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig().setIndexSort(timeSeriesIndexSort()));
                try {
                    writer.addDocument(storedFormatDocument.rootDoc());
                    writer.commit();
                    expectThrows(IllegalArgumentException.class, () -> writer.addDocument(docValuesFormatDocument.rootDoc()));
                    final NullPointerException poisonedSortField = expectThrows(NullPointerException.class, writer::commit);
                    assertThat(
                        poisonedSortField.getMessage(),
                        containsString(
                            "Cannot invoke \"org.apache.lucene.index.FieldInfo.getDocValuesType()\" because \"pf.fieldInfo\" is null"
                        )
                    );
                } finally {
                    IOUtils.closeWhileHandlingException(writer);
                }
            }
        }
    }

    public void testFormatFlipFailsReadPath() throws IOException {
        try (
            MapperService storedFormatMapperService = storedFormatMapperService();
            MapperService docValuesFormatMapperService = docValuesFormatMapperService()
        ) {
            final ParsedDocument storedFormatDocument = parseTimeSeriesDocument(storedFormatMapperService.documentMapper());

            withLuceneIndex(storedFormatMapperService, writer -> writer.addDocuments(storedFormatDocument.docs()), reader -> {
                final IllegalStateException mismatch = expectThrows(
                    IllegalStateException.class,
                    () -> syntheticSource(docValuesFormatMapperService.documentMapper(), reader, 0)
                );
                assertThat(
                    mismatch.getMessage(),
                    containsString("unexpected docvalues type NONE for field '" + IgnoredSourceFieldMapper.NAME + "'")
                );
            });
        }
    }

    public void testWriteAndReadAfterFlip() throws IOException {
        try (MapperService docValuesFormatMapperService = docValuesFormatMapperService()) {
            final ParsedDocument document = parseTimeSeriesDocument(docValuesFormatMapperService.documentMapper());

            withLuceneIndex(
                docValuesFormatMapperService,
                writer -> writer.addDocuments(document.docs()),
                reader -> assertThat(
                    syntheticSource(docValuesFormatMapperService.documentMapper(), reader, 0),
                    containsString("\"disabled_object\":{\"key\":\"value\"}")
                )
            );
        }
    }

    private MapperService storedFormatMapperService() throws IOException {
        final MapperService mapperService = createTimeSeriesMapperService(
            IndexVersionUtils.getPreviousVersion(IndexVersions.IGNORED_SOURCE_AS_DOC_VALUES)
        );
        assertEquals(
            IgnoredSourceFieldMapper.IgnoredSourceFormat.COALESCED_SINGLE_IGNORED_SOURCE,
            IgnoredSourceFieldMapper.ignoredSourceFormat(mapperService.getIndexSettings())
        );
        return mapperService;
    }

    private MapperService docValuesFormatMapperService() throws IOException {
        final MapperService mapperService = createTimeSeriesMapperService(IndexVersion.current());
        assertEquals(
            IgnoredSourceFieldMapper.IgnoredSourceFormat.DOC_VALUES_IGNORED_SOURCE,
            IgnoredSourceFieldMapper.ignoredSourceFormat(mapperService.getIndexSettings())
        );
        return mapperService;
    }

    private MapperService createTimeSeriesMapperService(final IndexVersion indexVersion) throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-10-29T00:00:00Z")
            .put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true)
            // NOTE: synthetic _id is unrelated to the format flip and its terms producer trips CheckIndex assertions here.
            .put(IndexSettings.SYNTHETIC_ID.getKey(), false)
            .build();
        return createMapperService(indexVersion, settings, mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("disabled_object").field("type", "object").field("enabled", false).endObject();
        }));
    }

    private ParsedDocument parseTimeSeriesDocument(final DocumentMapper documentMapper) throws IOException {
        final ParsedDocument document = documentMapper.parse(source(null, b -> {
            b.field("@timestamp", "2021-10-01");
            b.field("dim", "series-1");
            // NOTE: with synthetic source the content of a disabled object is kept only in _ignored_source,
            // so this value forces both mappers to write the _ignored_source field in their respective formats.
            b.startObject("disabled_object").field("key", "value").endObject();
        }, TimeSeriesRoutingHashFieldMapper.DUMMY_ENCODED_VALUE));
        assertNotNull(document.rootDoc().getField(IgnoredSourceFieldMapper.NAME));
        return document;
    }

    private static Sort timeSeriesIndexSort() {
        return new Sort(
            new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING, false),
            new SortedNumericSortField(DataStreamTimestampFieldMapper.DEFAULT_PATH, SortField.Type.LONG, true)
        );
    }
}
