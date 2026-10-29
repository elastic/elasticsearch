/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper.batch;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The {@link IndexWriter} the benchmark writes into, configured with the shape's codec and index sort. Merges are disabled and
 * the RAM buffer is large enough that an invocation never auto-flushes, and the sink never flushes explicitly, because segment
 * creation dominated the timed region and hid mapper changes; documents are dropped with {@code deleteAll} between invocations.
 */
public final class LuceneSink implements Closeable {

    private static final double RAM_BUFFER_MB = 1024;

    private final Path dir;
    private final Directory directory;
    private final IndexWriter writer;

    private LuceneSink(Path dir, Directory directory, IndexWriter writer) {
        this.dir = dir;
        this.directory = directory;
        this.writer = writer;
    }

    public static final class SeqNoStamper {

        private static final long PRIMARY_TERM = 1L;
        private static final long VERSION = 1L;

        private long nextSeqNo;

        public long nextSeqNo() {
            return nextSeqNo++;
        }

        public long primaryTerm() {
            return PRIMARY_TERM;
        }

        public long version() {
            return VERSION;
        }
    }

    public static LuceneSink openInTempDir(Codec codec, MapperService mapperService) throws IOException {
        Path dir = Files.createTempDirectory("lucene-sink");
        try {
            return open(codec, mapperService, dir);
        } catch (IOException | RuntimeException e) {
            try {
                IOUtils.rm(dir);
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    public static LuceneSink open(Codec codec, MapperService mapperService, Path dir) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCommitOnClose(false);
        iwc.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setCodec(codec);
        iwc.setRAMBufferSizeMB(RAM_BUFFER_MB);
        iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        Sort indexSort = indexSort(mapperService);
        if (indexSort != null) {
            iwc.setIndexSort(indexSort);
            if (mapperService.getIndexSettings().getIndexVersionCreated().onOrAfter(IndexVersions.INDEX_SORTING_ON_NESTED)) {
                iwc.setParentField(Engine.ROOT_DOC_FIELD_NAME);
            }
        }
        Directory directory = FSDirectory.open(dir);
        try {
            return new LuceneSink(dir, directory, new IndexWriter(directory, iwc));
        } catch (IOException | RuntimeException e) {
            IOUtils.closeWhileHandlingException(directory);
            throw e;
        }
    }

    static Sort indexSort(MapperService mapperService) {
        IndexSettings indexSettings = mapperService.getIndexSettings();
        IndexSortConfig sortConfig = indexSettings.getIndexSortConfig();
        if (sortConfig.hasIndexSort() == false) {
            return null;
        }
        FieldDataContext context = FieldDataContext.noRuntimeFields(indexSettings.getIndex().getName(), "index sort");
        return sortConfig.buildIndexSort(
            mapperService::fieldType,
            (fieldType, searchLookup) -> fieldType.fielddataBuilder(context)
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService())
        );
    }

    public IndexWriter writer() {
        return writer;
    }

    public void afterInvocation(int expectedDocs) throws IOException {
        requireDocs(expectedDocs);
        writer.deleteAll();
    }

    public void afterIteration() throws IOException {
        requireDocs(0);
        writer.commit();
    }

    private void requireDocs(int expectedDocs) {
        int actual = writer.getDocStats().numDocs;
        if (actual != expectedDocs) {
            throw new IllegalStateException("expected [" + expectedDocs + "] docs but the writer holds [" + actual + "]");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.close(writer, directory);
        } finally {
            IOUtils.rm(dir);
        }
    }
}
