/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.test.MockLog;
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

    // NOTE: runs ShardBatchMapper#mapColumnBatch itself, so that the metadata mappers, the required field check and the chunk context
    // are the ones the engine uses. It falls back to document indexing on any Exception and only logs it. A refused resolution and the
    // UnsupportedOperationException that mappers throw to ask for the fallback fall back here too. The logged exception of any other
    // failure, which is a mapper bug, fails the test instead of being hidden by document indexing. Returns why the batch was refused,
    // or null when it was indexed.
    private static String indexAsBatch(MapperService mapperService, IndexWriter indexWriter, List<String> sources, int firstSeqNo)
        throws IOException {
        final int count = sources.size();
        final BytesReference[] bytes = new BytesReference[count];
        final BulkItemRequest[] items = new BulkItemRequest[count];
        for (int i = 0; i < count; i++) {
            bytes[i] = new BytesArray(sources.get(i).getBytes(StandardCharsets.UTF_8));
            items[i] = new BulkItemRequest(i, new IndexRequest("index").id("d" + (firstSeqNo + i)).source(bytes[i], XContentType.JSON));
        }
        final MappingLookup lookup = mapperService.mappingLookup();
        final IndexSettings indexSettings = mapperService.getIndexSettings();
        try (EscfBatch escfBatch = EscfEncoder.encode(Arrays.asList(bytes), XContentType.JSON)) {
            final ShardBatchMapper.BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(escfBatch, lookup, indexSettings);
            if (resolution == null) {
                return "resolveMappers refused the batch";
            }
            final FailureRecorder failure = new FailureRecorder();
            final EngineBatch engineBatch;
            try (MockLog mockLog = MockLog.capture(ShardBatchMapper.class)) {
                mockLog.addExpectation(failure);
                engineBatch = ShardBatchMapper.mapColumnBatch(
                    items,
                    escfBatch,
                    new TestShardContext(mapperService),
                    0,
                    count,
                    resolution,
                    Engine.Operation.Origin.PRIMARY,
                    new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY)),
                    DocumentParsingProvider.EMPTY_INSTANCE
                );
                mockLog.assertAllExpectationsMatched();
            }
            if (engineBatch == null) {
                if (failure.thrown instanceof UnsupportedOperationException) {
                    return String.valueOf(failure.thrown.getMessage());
                }
                throw new AssertionError("the mappers failed to map a batch, which is not a request to fall back", failure.thrown);
            }
            try (engineBatch) {
                final MappedColumns columns = engineBatch.columns();
                columns.fillPrimaryTerm(1L);
                for (int i = 0; i < count; i++) {
                    columns.setSeqNo(i, firstSeqNo + i);
                    columns.setVersion(i, 1L);
                }
                indexWriter.addBatch(columns.toColumnBatch());
            }
            return null;
        }
    }

    private static final class FailureRecorder implements MockLog.LoggingExpectation {
        private volatile Throwable thrown;

        @Override
        public void match(LogEvent event) {
            if (event.getThrown() != null) {
                thrown = event.getThrown();
            }
        }

        @Override
        public void assertMatched() {}
    }

    private record TestShardContext(MapperService mapperService) implements BatchMappingShardContext {
        @Override
        public IndexSettings indexSettings() {
            return mapperService.getIndexSettings();
        }

        @Override
        public long getOperationPrimaryTerm() {
            return 1L;
        }

        @Override
        public long getRelativeTimeInNanos() {
            return 0L;
        }
    }
}
