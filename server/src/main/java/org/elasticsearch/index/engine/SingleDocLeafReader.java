/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
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
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

/**
 * Internal class that mocks a single doc read from the transaction log or an in-memory index as a leaf reader.
 */
public final class SingleDocLeafReader extends LeafReader {

    private static final FieldInfo FAKE_SOURCE_FIELD
        = new FieldInfo(SourceFieldMapper.NAME, 1, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, 0, false);
    private static final FieldInfo FAKE_ROUTING_FIELD
        = new FieldInfo(RoutingFieldMapper.NAME, 2, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, 0, false);
    private static final FieldInfo FAKE_ID_FIELD
        = new FieldInfo(IdFieldMapper.NAME, 3, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, 0, false);
    public static final Set<String> ALL_FIELD_NAMES = Set.of(FAKE_SOURCE_FIELD.name, FAKE_ROUTING_FIELD.name, FAKE_ID_FIELD.name);


    private final ShardId shardId;
    private final Translog.Index operation;
    private final DocumentMapper mapper;
    private final Analyzer analyzer;
    private final ByteBuffersDirectory directory;
    private final SetOnce<LeafReader> luceneReaderHolder = new SetOnce<>();

    SingleDocLeafReader(ShardId shardId, Translog.Index operation, DocumentMapper mapper, Analyzer analyzer) {
        this.shardId = shardId;
        this.operation = operation;
        this.mapper = mapper;
        this.analyzer = analyzer;
        this.directory = new ByteBuffersDirectory();
    }

    private LeafReader getOrCreateLuceneReader() {
        LeafReader reader = luceneReaderHolder.get();
        if (reader == null) {
            synchronized (this) {
                reader = luceneReaderHolder.get();
                if (reader == null) {
                    reader = createLuceneLeafReader();
                    luceneReaderHolder.set(reader);
                }
            }
        }
        return reader;
    }

    private LeafReader createLuceneLeafReader() {
        assert Thread.holdsLock(this);
        final ParsedDocument parsedDocs = mapper.parse(new SourceToParse(shardId.getIndexName(), operation.id(),
            operation.source(), XContentHelper.xContentType(operation.source()), operation.routing()));
        parsedDocs.updateSeqID(operation.seqNo(), operation.primaryTerm());
        parsedDocs.version().setLongValue(operation.version());
        final IndexWriterConfig writeConfig = new IndexWriterConfig(analyzer).setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        try (IndexWriter writer = new IndexWriter(directory, writeConfig)) {
            writer.addDocument(parsedDocs.rootDoc());
            final DirectoryReader reader = DirectoryReader.open(writer);
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

    public Directory getDirectory() {
        return directory;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return getOrCreateLuceneReader().getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return getOrCreateLuceneReader().getReaderCacheHelper();
    }

    @Override
    public Terms terms(String field) throws IOException {
        return getOrCreateLuceneReader().terms(field);
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        return getOrCreateLuceneReader().getNumericDocValues(field);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        return getOrCreateLuceneReader().getBinaryDocValues(field);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        return getOrCreateLuceneReader().getSortedDocValues(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        return getOrCreateLuceneReader().getSortedNumericDocValues(field);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        return getOrCreateLuceneReader().getSortedSetDocValues(field);
    }

    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
        return getOrCreateLuceneReader().getNormValues(field);
    }

    @Override
    public FieldInfos getFieldInfos() {
        return getOrCreateLuceneReader().getFieldInfos();
    }

    @Override
    public Bits getLiveDocs() {
        return getOrCreateLuceneReader().getLiveDocs();
    }

    @Override
    public PointValues getPointValues(String field) throws IOException {
        return getOrCreateLuceneReader().getPointValues(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
    }

    @Override
    public LeafMetaData getMetaData() {
        return getOrCreateLuceneReader().getMetaData();
    }

    @Override
    public Fields getTermVectors(int docID) throws IOException {
        return getOrCreateLuceneReader().getTermVectors(docID);
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
        if (docID != 0) {
            throw new IllegalArgumentException("no such doc ID " + docID);
        }
        final LeafReader luceneReader = luceneReaderHolder.get();
        if (luceneReader != null) {
            luceneReader.document(docID, visitor);
        } else {
            readFromTranslog(visitor);
        }
    }

    private void readFromTranslog(StoredFieldVisitor visitor) throws IOException {
        if (visitor.needsField(FAKE_SOURCE_FIELD) == StoredFieldVisitor.Status.YES) {
            assert operation.source().toBytesRef().offset == 0;
            assert operation.source().toBytesRef().length == operation.source().toBytesRef().bytes.length;
            visitor.binaryField(FAKE_SOURCE_FIELD, operation.source().toBytesRef().bytes);
        }
        if (operation.routing() != null && visitor.needsField(FAKE_ROUTING_FIELD) == StoredFieldVisitor.Status.YES) {
            visitor.stringField(FAKE_ROUTING_FIELD, operation.routing().getBytes(StandardCharsets.UTF_8));
        }
        if (visitor.needsField(FAKE_ID_FIELD) == StoredFieldVisitor.Status.YES) {
            BytesRef bytesRef = Uid.encodeId(operation.id());
            final byte[] id = new byte[bytesRef.length];
            System.arraycopy(bytesRef.bytes, bytesRef.offset, id, 0, bytesRef.length);
            visitor.binaryField(FAKE_ID_FIELD, id);
        }
    }

    @Override
    protected void doClose() throws IOException {
        directory.close();
    }
}
