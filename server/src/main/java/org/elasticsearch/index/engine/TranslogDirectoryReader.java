/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.fieldvisitor.FieldNamesProvidingStoredFieldsVisitor;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link DirectoryReader} that contains a single translog indexing operation.
 * This can be used during a realtime get to access documents that haven't been refreshed yet.
 * In the normal case, all information relevant to resolve the realtime get is mocked out
 * to provide fast access to _id and _source. In case where more values are requested
 * (e.g. access to other stored fields) etc., this reader will index the document
 * into an in-memory Lucene segment that is created on-demand.
 */
final class TranslogDirectoryReader extends DirectoryReader {
    private final TranslogLeafReader leafReader;

    TranslogDirectoryReader(ShardId shardId, Translog.Index operation, MappingLookup mappingLookup, DocumentParser documentParser,
                            Analyzer analyzer, Runnable onSegmentCreated) throws IOException {
        this(new TranslogLeafReader(shardId, operation, mappingLookup, documentParser, analyzer, onSegmentCreated));
    }

    private TranslogDirectoryReader(TranslogLeafReader leafReader) throws IOException {
        super(leafReader.directory, new LeafReader[]{leafReader}, null);
        this.leafReader = leafReader;
    }

    private static UnsupportedOperationException unsupported() {
        assert false : "unsupported operation";
        return new UnsupportedOperationException();
    }

    public TranslogLeafReader getLeafReader() {
        return leafReader;
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

    private static class TranslogLeafReader extends LeafReader {

        private static final FieldInfo FAKE_SOURCE_FIELD
            = new FieldInfo(SourceFieldMapper.NAME, 1, false, false, false, IndexOptions.NONE,
            DocValuesType.NONE, -1, Collections.emptyMap(), 0, 0, 0, false);
        private static final FieldInfo FAKE_ROUTING_FIELD
            = new FieldInfo(RoutingFieldMapper.NAME, 2, false, false, false, IndexOptions.NONE,
            DocValuesType.NONE, -1, Collections.emptyMap(), 0, 0, 0, false);
        private static final FieldInfo FAKE_ID_FIELD
            = new FieldInfo(IdFieldMapper.NAME, 3, false, false, false, IndexOptions.DOCS,
            DocValuesType.NONE, -1, Collections.emptyMap(), 0, 0, 0, false);
        private static Set<String> TRANSLOG_FIELD_NAMES =
            Sets.newHashSet(SourceFieldMapper.NAME, RoutingFieldMapper.NAME, IdFieldMapper.NAME);


        private final ShardId shardId;
        private final Translog.Index operation;
        private final MappingLookup mappingLookup;
        private final DocumentParser documentParser;
        private final Analyzer analyzer;
        private final Directory directory;
        private final Runnable onSegmentCreated;

        private final AtomicReference<LeafReader> delegate = new AtomicReference<>();
        private final BytesRef uid;

        TranslogLeafReader(ShardId shardId, Translog.Index operation, MappingLookup mappingLookup, DocumentParser documentParser,
                           Analyzer analyzer, Runnable onSegmentCreated) {
            this.shardId = shardId;
            this.operation = operation;
            this.mappingLookup = mappingLookup;
            this.documentParser = documentParser;
            this.analyzer = analyzer;
            this.onSegmentCreated = onSegmentCreated;
            this.directory = new ByteBuffersDirectory();
            this.uid = Uid.encodeId(operation.id());
        }

        private LeafReader getDelegate() {
            ensureOpen();
            LeafReader reader = delegate.get();
            if (reader == null) {
                synchronized (this) {
                    ensureOpen();
                    reader = delegate.get();
                    if (reader == null) {
                        reader = createInMemoryLeafReader();
                        final LeafReader existing = delegate.getAndSet(reader);
                        assert existing == null;
                        onSegmentCreated.run();
                    }
                }
            }
            return reader;
        }

        private LeafReader createInMemoryLeafReader() {
            assert Thread.holdsLock(this);
            final SourceToParse sourceToParse = SourceToParse.parseTimeSeriesIdFromSource(
                shardId.getIndexName(),
                operation.id(),
                operation.source(),
                XContentHelper.xContentType(operation.source()),
                operation.routing(),
                Map.of(),
                mappingLookup
            );
            final ParsedDocument parsedDocs = documentParser.parseDocument(sourceToParse, mappingLookup);

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
            return null;
        }

        @Override
        public Terms terms(String field) throws IOException {
            if (delegate.get() == null) {
                // override this for VersionsAndSeqNoResolver
                if (field.equals(IdFieldMapper.NAME)) {
                    return new FakeTerms(uid);
                }
            }
            return getDelegate().terms(field);
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            if (delegate.get() == null) {
                // override this for VersionsAndSeqNoResolver
                if (field.equals(VersionFieldMapper.NAME)) {
                    return new FakeNumericDocValues(operation.version());
                }
                if (field.equals(SeqNoFieldMapper.NAME)) {
                    return new FakeNumericDocValues(operation.seqNo());
                }
                if (field.equals(SeqNoFieldMapper.PRIMARY_TERM_NAME)) {
                    return new FakeNumericDocValues(operation.primaryTerm());
                }
            }
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
        public FieldInfos getFieldInfos() {
            return getDelegate().getFieldInfos();
        }

        @Override
        public Bits getLiveDocs() {
            return null;
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
        public Fields getTermVectors(int docID) throws IOException {
            return getDelegate().getTermVectors(docID);
        }

        @Override
        public int numDocs() {
            return 1;
        }

        @Override
        public int maxDoc() {
            return 1;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            assert docID == 0;
            if (docID != 0) {
                throw new IllegalArgumentException("no such doc ID " + docID);
            }
            if (delegate.get() == null) {
                if (visitor instanceof FieldNamesProvidingStoredFieldsVisitor) {
                    // override this for ShardGetService
                    if (TRANSLOG_FIELD_NAMES.containsAll(((FieldNamesProvidingStoredFieldsVisitor) visitor).getFieldNames())) {
                        readStoredFieldsDirectly(visitor);
                        return;
                    }
                }
            }

            getDelegate().document(docID, visitor);
        }

        private void readStoredFieldsDirectly(StoredFieldVisitor visitor) throws IOException {
            if (visitor.needsField(FAKE_SOURCE_FIELD) == StoredFieldVisitor.Status.YES) {
                BytesReference sourceBytes = operation.source();
                assert BytesReference.toBytes(sourceBytes) == sourceBytes.toBytesRef().bytes;
                SourceFieldMapper mapper = mappingLookup.getMapping().getMetadataMapperByClass(SourceFieldMapper.class);
                if (mapper != null) {
                    try {
                        sourceBytes = mapper.applyFilters(sourceBytes, null);
                    } catch (IOException e) {
                        throw new IOException("Failed to reapply filters after reading from translog", e);
                    }
                }
                if (sourceBytes != null) {
                    visitor.binaryField(FAKE_SOURCE_FIELD, BytesReference.toBytes(sourceBytes));
                }
            }
            if (operation.routing() != null && visitor.needsField(FAKE_ROUTING_FIELD) == StoredFieldVisitor.Status.YES) {
                visitor.stringField(FAKE_ROUTING_FIELD, operation.routing().getBytes(StandardCharsets.UTF_8));
            }
            if (visitor.needsField(FAKE_ID_FIELD) == StoredFieldVisitor.Status.YES) {
                final byte[] id = new byte[uid.length];
                System.arraycopy(uid.bytes, uid.offset, id, 0, uid.length);
                visitor.binaryField(FAKE_ID_FIELD, id);
            }
        }

        @Override
        protected synchronized void doClose() throws IOException {
            IOUtils.close(delegate.get(), directory);
        }
    }

    private static class FakeTerms extends Terms {
        private final BytesRef uid;

        FakeTerms(BytesRef uid) {
            this.uid = uid;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new FakeTermsEnum(uid);
        }

        @Override
        public long size() throws IOException {
            return 1;
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            return 1;
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return 1;
        }

        @Override
        public int getDocCount() throws IOException {
            return 1;
        }

        @Override
        public boolean hasFreqs() {
            return false;
        }

        @Override
        public boolean hasOffsets() {
            return false;
        }

        @Override
        public boolean hasPositions() {
            return false;
        }

        @Override
        public boolean hasPayloads() {
            return false;
        }
    }

    private static class FakeTermsEnum extends BaseTermsEnum {
        private final BytesRef term;
        private long position = -1;

        FakeTermsEnum(BytesRef term) {
            this.term = term;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            int cmp = text.compareTo(term);
            if (cmp == 0) {
                position = 0;
                return SeekStatus.FOUND;
            } else if (cmp < 0) {
                position = 0;
                return SeekStatus.NOT_FOUND;
            }
            position = Long.MAX_VALUE;
            return SeekStatus.END;
        }

        @Override
        public void seekExact(long ord) throws IOException {
            position = ord;
        }

        @Override
        public BytesRef term() throws IOException {
            assert position == 0;
            return term;
        }

        @Override
        public long ord() throws IOException {
            return position;
        }

        @Override
        public int docFreq() throws IOException {
            return 1;
        }

        @Override
        public long totalTermFreq() throws IOException {
            return 1;
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            return new FakePostingsEnum(term);
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            throw unsupported();
        }

        @Override
        public BytesRef next() throws IOException {
            return ++position == 0 ? term : null;
        }
    }

    private static class FakePostingsEnum extends PostingsEnum {
        private final BytesRef term;

        private int iter = -1;

        private FakePostingsEnum(BytesRef term) {
            this.term = term;
        }

        @Override
        public int freq() {
            return 1;
        }

        @Override
        public int nextPosition() {
            return 0;
        }

        @Override
        public int startOffset() {
            return 0;
        }

        @Override
        public int endOffset() {
            return term.length;
        }

        @Override
        public BytesRef getPayload() {
            return null;
        }

        @Override
        public int docID() {
            return iter > 0 ? NO_MORE_DOCS : iter;
        }

        @Override
        public int nextDoc() {
            return ++iter == 0 ? 0 : NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            int doc;
            while ((doc = nextDoc()) < target) {
            }
            return doc;
        }

        @Override
        public long cost() {
            return 0;
        }
    }

    private static class FakeNumericDocValues extends NumericDocValues {
        private final long value;
        private final DocIdSetIterator disi = DocIdSetIterator.all(1);

        FakeNumericDocValues(long value) {
            this.value = value;
        }

        @Override
        public long longValue() {
            return value;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return disi.advance(target) == target;
        }

        @Override
        public int docID() {
            return disi.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return disi.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return disi.advance(target);
        }

        @Override
        public long cost() {
            return disi.cost();
        }
    }
}
