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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * How a test indexes its documents. {@link #DOCUMENT} adds each parsed document with {@code IndexWriter#addDocuments}. {@link #BATCH}
 * maps whole columns and adds them with {@code IndexWriter#addBatch}, the way the engine does, and falls back to document indexing when
 * the mappers refuse the batch, as production does. The {@link Outcome} says which one executed, so that a fallback is never taken
 * for batch coverage.
 */
public enum IndexingStrategy {
    DOCUMENT {
        @Override
        public Outcome index(MapperService mapperService, IndexWriter indexWriter, List<String> sources, int firstSeqNo)
            throws IOException {
            for (int i = 0; i < sources.size(); i++) {
                final SourceToParse source = new SourceToParse("d" + (firstSeqNo + i), new BytesArray(sources.get(i)), XContentType.JSON);
                final ParsedDocument parsed = mapperService.documentMapper().parse(source);
                parsed.updateSeqID(firstSeqNo + i, 1);
                parsed.version().setLongValue(1);
                indexWriter.addDocuments(parsed.docs());
            }
            return new Outcome(false, null);
        }
    },
    BATCH {
        @Override
        public Outcome index(MapperService mapperService, IndexWriter indexWriter, List<String> sources, int firstSeqNo)
            throws IOException {
            final String fallbackReason = indexAsBatch(mapperService, indexWriter, sources, firstSeqNo);
            if (fallbackReason == null) {
                return new Outcome(true, null);
            }
            DOCUMENT.index(mapperService, indexWriter, sources, firstSeqNo);
            return new Outcome(false, fallbackReason);
        }
    };

    /**
     * @param batched        whether the documents were added with {@code IndexWriter#addBatch}
     * @param fallbackReason why {@link #BATCH} fell back to document indexing, or {@code null} when it did not
     */
    public record Outcome(boolean batched, String fallbackReason) {}

    /**
     * Indexes the sources as documents {@code d<firstSeqNo>} onwards, with sequence numbers starting at {@code firstSeqNo}, primary term 1
     * and version 1. {@code BATCH} lets any exception other than a refusal to batch propagate.
     */
    public abstract Outcome index(MapperService mapperService, IndexWriter indexWriter, List<String> sources, int firstSeqNo)
        throws IOException;

    // NOTE: mirrors ShardBatchMapper#mapColumnBatch, which falls back to document indexing on any Exception and logs it. Only a refused
    // resolution and the UnsupportedOperationException that mappers throw to ask for the fallback do so here, so that any other exception,
    // which is a mapper bug, fails the test instead of being hidden by document indexing. The metadata mappers run outside the try for the
    // same reason. Returns why the batch was refused, or null when it was indexed.
    private static String indexAsBatch(MapperService mapperService, IndexWriter indexWriter, List<String> sources, int firstSeqNo)
        throws IOException {
        final int count = sources.size();
        final BytesReference[] bytes = new BytesReference[count];
        final IndexRequest[] requests = new IndexRequest[count];
        for (int i = 0; i < count; i++) {
            bytes[i] = new BytesArray(sources.get(i).getBytes(StandardCharsets.UTF_8));
            requests[i] = new IndexRequest("index").id("d" + (firstSeqNo + i)).source(bytes[i], XContentType.JSON);
        }
        final MappingLookup lookup = mapperService.mappingLookup();
        final IndexSettings indexSettings = mapperService.getIndexSettings();
        try (
            BatchMappingContext context = new BatchMappingContext(
                EngineTestCase.initFromRequests(requests),
                lookup,
                indexSettings,
                new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY))
            );
            EscfBatch escfBatch = EscfEncoder.encode(Arrays.asList(bytes), XContentType.JSON)
        ) {
            final ShardBatchMapper.BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(escfBatch, lookup, indexSettings);
            if (resolution == null) {
                return "resolveMappers refused the batch";
            }
            final MetadataFieldMapper[] metadataMappers = lookup.getMapping().getSortedMetadataMappers();
            for (final MetadataFieldMapper metadataMapper : metadataMappers) {
                metadataMapper.preColumnarParse(context);
            }
            try {
                final FieldMapper[] columnMappers = resolution.columnMappers();
                for (int c = 0; c < columnMappers.length; c++) {
                    if (columnMappers[c] != null) {
                        columnMappers[c].mapColumnBatch(context, escfBatch.column(c));
                    }
                }
                for (final ColumnGroupResolver.ColumnGroupResolution group : resolution.columnGroups()) {
                    final int[] leafIndexes = group.leafIndexes();
                    final EscfColumn[] groupColumns = new EscfColumn[leafIndexes.length];
                    for (int i = 0; i < leafIndexes.length; i++) {
                        groupColumns[i] = escfBatch.column(leafIndexes[i]);
                    }
                    group.mapper().mapColumnGroupBatch(context, groupColumns, group.relativeKeys());
                }
            } catch (UnsupportedOperationException e) {
                return String.valueOf(e.getMessage());
            }
            for (final MetadataFieldMapper metadataMapper : metadataMappers) {
                metadataMapper.postColumnarParse(context);
            }
            final MappedColumns columns = context.columns();
            columns.fillPrimaryTerm(1L);
            for (int i = 0; i < count; i++) {
                columns.setSeqNo(i, firstSeqNo + i);
                columns.setVersion(i, 1L);
            }
            indexWriter.addBatch(columns.toColumnBatch());
            return null;
        }
    }
}
