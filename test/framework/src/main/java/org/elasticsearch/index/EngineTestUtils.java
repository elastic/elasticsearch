/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.engine.DocIdSeqNoAndSource;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.shard.IndexShard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public final class EngineTestUtils {

    private EngineTestUtils() {}

    /**
     * Gets a collection of tuples of docId, sequence number, and primary term of all live documents in the provided engine.
     *
     * Note: this method has limited support for reading document fields and should not be used outside of engine unit tests. For example,
     * it does not support synthetic fields or reading documents from a {@link org.elasticsearch.index.engine.NoOpEngine}.
     *
     * For integration tests, use the {@link org.elasticsearch.test.ESIntegTestCase#getDocIdAndSeqNos(IndexShard)} method.
     */
    public static List<DocIdSeqNoAndSource> getDocIds(Engine engine, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test_get_doc_ids");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test_get_doc_ids", Engine.SearcherScope.INTERNAL)) {
            List<DocIdSeqNoAndSource> docs = new ArrayList<>();
            for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
                LeafReader reader = leafContext.reader();
                NumericDocValues seqNoDocValues = reader.getNumericDocValues(SeqNoFieldMapper.NAME);
                NumericDocValues primaryTermDocValues = reader.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
                NumericDocValues versionDocValues = reader.getNumericDocValues(VersionFieldMapper.NAME);
                Bits liveDocs = reader.getLiveDocs();
                StoredFields storedFields = reader.storedFields();
                for (int i = 0; i < reader.maxDoc(); i++) {
                    if (liveDocs == null || liveDocs.get(i)) {
                        if (primaryTermDocValues.advanceExact(i) == false) {
                            // We have to skip non-root docs because its _id field is not stored (indexed only).
                            continue;
                        }
                        final long primaryTerm = primaryTermDocValues.longValue();
                        Document doc = storedFields.document(i, Set.of(IdFieldMapper.NAME, SourceFieldMapper.NAME));
                        BytesRef binaryID = doc.getBinaryValue(IdFieldMapper.NAME);
                        String id = Uid.decodeId(Arrays.copyOfRange(binaryID.bytes, binaryID.offset, binaryID.offset + binaryID.length));
                        final BytesRef source = doc.getBinaryValue(SourceFieldMapper.NAME);
                        if (seqNoDocValues.advanceExact(i) == false) {
                            throw new AssertionError("seqNoDocValues not found for doc[" + i + "] id[" + id + "]");
                        }
                        final long seqNo = seqNoDocValues.longValue();
                        if (versionDocValues.advanceExact(i) == false) {
                            throw new AssertionError("versionDocValues not found for doc[" + i + "] id[" + id + "]");
                        }
                        final long version = versionDocValues.longValue();
                        docs.add(new DocIdSeqNoAndSource(id, source, seqNo, primaryTerm, version));
                    }
                }
            }
            docs.sort(
                Comparator.comparingLong(DocIdSeqNoAndSource::seqNo)
                    .thenComparingLong(DocIdSeqNoAndSource::primaryTerm)
                    .thenComparing((DocIdSeqNoAndSource::id))
            );
            return docs;
        }
    }
}
