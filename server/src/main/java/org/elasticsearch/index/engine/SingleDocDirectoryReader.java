/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link DirectoryReader} contains a single leaf reader delegating to an in-memory Lucene segment that is lazily created from
 * a single document.
 */
final class SingleDocDirectoryReader extends DirectoryReader {
    private final SingleDocLeafReader leafReader;

    SingleDocDirectoryReader(ShardId shardId, Translog.Index operation, MappingLookup mappingLookup, DocumentParser documentParser,
                             Analyzer analyzer) throws IOException {
        this(new SingleDocLeafReader(shardId, operation, mappingLookup, documentParser, analyzer));
    }

    private SingleDocDirectoryReader(SingleDocLeafReader leafReader) throws IOException {
        super(leafReader.directory, new LeafReader[]{leafReader}, null);
        this.leafReader = leafReader;
    }

    boolean assertMemorySegmentStatus(boolean loaded) {
        return leafReader.assertMemorySegmentStatus(loaded);
    }

    private static UnsupportedOperationException unsupported() {
        assert false : "unsupported operation";
        return new UnsupportedOperationException();
    }

    @Override
    protected DirectoryReader doOpenIfChanged() {
        throw unsupported();
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexCommit commit) {
        throw unsupported();
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) {
        throw unsupported();
    }

    @Override
    public long getVersion() {
        throw unsupported();
    }

    @Override
    public boolean isCurrent() {
        throw unsupported();
    }

    @Override
    public IndexCommit getIndexCommit() {
        throw unsupported();
    }

    @Override
    protected void doClose() throws IOException {
        leafReader.close();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return leafReader.getReaderCacheHelper();
    }

    private static class SingleDocLeafReader extends LeafReader {

        private final ShardId shardId;
        private final Translog.Index operation;
        private final MappingLookup mappingLookup;
        private final DocumentParser documentParser;
        private final Analyzer analyzer;
        private final Directory directory;
        private final AtomicReference<LeafReader> delegate = new AtomicReference<>();

        SingleDocLeafReader(ShardId shardId, Translog.Index operation, MappingLookup mappingLookup, DocumentParser documentParser,
                            Analyzer analyzer) {
            this.shardId = shardId;
            this.operation = operation;
            this.mappingLookup = mappingLookup;
            this.documentParser = documentParser;
            this.analyzer = analyzer;
            this.directory = new ByteBuffersDirectory();
        }

        private LeafReader getDelegate() {
            ensureOpen();
            LeafReader reader = delegate.get();
            if (reader == null) {
                synchronized (this) {
                    reader = delegate.get();
                    if (reader == null) {
                        reader = createInMemoryLeafReader();
                        final LeafReader existing = delegate.getAndSet(reader);
                        assert existing == null;
                    }
                }
            }
            return reader;
        }

        private LeafReader createInMemoryLeafReader() {
            assert Thread.holdsLock(this);
            final ParsedDocument parsedDocs = documentParser.parseDocument(new SourceToParse(shardId.getIndexName(), operation.id(),
                operation.source(), XContentHelper.xContentType(operation.source()), operation.routing(), Map.of()), mappingLookup);

            parsedDocs.updateSeqID(operation.seqNo(), operation.primaryTerm());
            parsedDocs.version().setLongValue(operation.version());
            final IndexWriterConfig writeConfig = new IndexWriterConfig(analyzer).setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            try (IndexWriter writer = new IndexWriter(directory, writeConfig)) {
                writer.addDocument(parsedDocs.rootDoc());
                final DirectoryReader reader = open(writer);
                if (reader.leaves().size() != 1 || reader.leaves().get(0).reader().numDocs() != 1) {
                    reader.close();
                    throw new IllegalStateException("Expected a single document segment; " +
                        "but [" + reader.leaves().size() + " segments with " + reader.leaves().get(0).reader().numDocs() + " documents");
                }
                return reader.leaves().get(0).reader();
            } catch (IOException e) {
                throw new EngineException(shardId, "failed to create an in-memory segment for get [" + operation.id() + "]", e);
            }
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return getDelegate().getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return getDelegate().getReaderCacheHelper();
        }

        @Override
        public Terms terms(String field) throws IOException {
            return getDelegate().terms(field);
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            return getDelegate().getNumericDocValues(field);
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            return getDelegate().getBinaryDocValues(field);
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            return getDelegate().getSortedDocValues(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            return getDelegate().getSortedNumericDocValues(field);
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            return getDelegate().getSortedSetDocValues(field);
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            return getDelegate().getNormValues(field);
        }

        @Override
        public VectorValues getVectorValues(String field) throws IOException {
            return getDelegate().getVectorValues(field);
        }

        @Override
        public TopDocs searchNearestVectors(String field, float[] target, int k) throws IOException {
            return getDelegate().searchNearestVectors(field, target, k);
        }

        @Override
        public FieldInfos getFieldInfos() {
            return getDelegate().getFieldInfos();
        }

        @Override
        public Bits getLiveDocs() {
            return getDelegate().getLiveDocs();
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            return getDelegate().getPointValues(field);
        }

        @Override
        public void checkIntegrity() throws IOException {
        }

        @Override
        public LeafMetaData getMetaData() {
            return getDelegate().getMetaData();
        }

        @Override
        public TermVectors getTermVectorsReader() {
            return getDelegate().getTermVectorsReader();
        }

        @Override
        public int numDocs() {
            return 1;
        }

        @Override
        public int maxDoc() {
            return 1;
        }

        synchronized boolean assertMemorySegmentStatus(boolean loaded) {
            if (loaded) {
                assert delegate.get() != null :
                    "Expected an in memory segment was loaded; but it wasn't. Please check the reader wrapper implementation";
            } else {
                assert delegate.get() == null :
                    "Expected an in memory segment wasn't loaded; but it was. Please check the reader wrapper implementation";
            }
            return true;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            assert assertMemorySegmentStatus(true);
            getDelegate().document(docID, visitor);
        }

        @Override
        protected void doClose() throws IOException {
            IOUtils.close(delegate.get(), directory);
        }
    }
}
