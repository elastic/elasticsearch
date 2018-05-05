/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.translog.Translog;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * A {@link Translog.Snapshot} from changes in a Lucene index
 */
final class LuceneChangesSnapshot implements Translog.Snapshot {
    private final long fromSeqNo, toSeqNo;
    private long lastSeenSeqNo;
    private int skippedOperations;
    private final boolean requiredFullRange;

    private final IndexSearcher searcher;
    private final MapperService mapperService;
    private int docIndex;
    private final TopDocs topDocs;

    private final Closeable onClose;

    /**
     * Creates a new "translog" snapshot from Lucene for reading operations whose seq# in the specified range.
     *
     * @param searcherFactory   the engine searcher factory (prefer the internal searcher)
     * @param mapperService     the mapper service which will be mainly used to resolve the document's type and uid
     * @param fromSeqNo         the min requesting seq# - inclusive
     * @param toSeqNo           the maximum requesting seq# - inclusive
     * @param requiredFullRange if true, the snapshot will strictly check for the existence of operations between fromSeqNo and toSeqNo
     * @param onClose           a callback to be called when this snapshot is closed
     */
    LuceneChangesSnapshot(Supplier<Engine.Searcher> searcherFactory, MapperService mapperService,
                          long fromSeqNo, long toSeqNo, boolean requiredFullRange, Closeable onClose) throws IOException {
        if (fromSeqNo < 0 || toSeqNo < 0 || fromSeqNo > toSeqNo) {
            throw new IllegalArgumentException("Invalid range; from_seqno [" + fromSeqNo + "], to_seqno [" + toSeqNo + "]");
        }
        this.mapperService = mapperService;
        this.fromSeqNo = fromSeqNo;
        this.toSeqNo = toSeqNo;
        this.lastSeenSeqNo = fromSeqNo - 1;
        this.requiredFullRange = requiredFullRange;
        boolean success = false;
        final Engine.Searcher engineSearcher = searcherFactory.get();
        try {
            this.searcher = new IndexSearcher(Lucene.wrapAllDocsLive(engineSearcher.getDirectoryReader()));
            this.searcher.setQueryCache(null);
            this.topDocs = searchOperations(searcher);
            success = true;
            this.onClose = () -> IOUtils.close(onClose, engineSearcher);
        } finally {
            if (success == false) {
                IOUtils.close(engineSearcher);
            }
        }
    }

    @Override
    public void close() throws IOException {
        onClose.close();
    }

    @Override
    public int totalOperations() {
        return Math.toIntExact(topDocs.totalHits);
    }

    @Override
    public int overriddenOperations() {
        return skippedOperations;
    }

    @Override
    public Translog.Operation next() throws IOException {
        final Translog.Operation op = nextOp();
        if (requiredFullRange && lastSeenSeqNo < toSeqNo) {
            final long expectedSeqNo = lastSeenSeqNo + 1;
            if (op == null || op.seqNo() != expectedSeqNo) {
                throw new IllegalStateException("Not all operations between min_seqno [" + fromSeqNo + "] " +
                    "and max_seqno [" + toSeqNo + "] found; expected seqno [" + expectedSeqNo + "]; found [" + op + "]");
            }
        }
        if (op != null) {
            lastSeenSeqNo = op.seqNo();
        }
        return op;
    }

    private Translog.Operation nextOp() throws IOException {
        final ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (; docIndex < scoreDocs.length; docIndex++) {
            if (scoreDocs[docIndex].doc == DocIdSetIterator.NO_MORE_DOCS) {
                return null;
            }
            final Translog.Operation op = readDocAsOp(scoreDocs[docIndex].doc);
            if (op != null) {
                return op;
            }
        }
        return null;
    }

    private TopDocs searchOperations(IndexSearcher searcher) throws IOException {
        final Query rangeQuery = LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, fromSeqNo, toSeqNo);
        final Sort sortedBySeqNoThenByTerm = new Sort(
            new SortedNumericSortField(SeqNoFieldMapper.NAME, SortField.Type.LONG),
            new SortedNumericSortField(SeqNoFieldMapper.PRIMARY_TERM_NAME, SortField.Type.LONG, true)
        );
        return searcher.search(rangeQuery, Integer.MAX_VALUE, sortedBySeqNoThenByTerm);
    }

    private Translog.Operation readDocAsOp(int docID) throws IOException {
        final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        final LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(docID, leaves));
        final int segmentDocID = docID - leaf.docBase;
        final long seqNo = readNumericDV(leaf, SeqNoFieldMapper.NAME, segmentDocID);
        // This operation has seen and will be skipped anyway - do not visit other fields.
        if (seqNo == lastSeenSeqNo) {
            skippedOperations++;
            return null;
        }

        final long primaryTerm = readNumericDV(leaf, SeqNoFieldMapper.PRIMARY_TERM_NAME, segmentDocID);
        final FieldsVisitor fields = new FieldsVisitor(true);
        searcher.doc(docID, fields);
        fields.postProcess(mapperService);

        final Translog.Operation op;
        final boolean isTombstone = isTombstoneOperation(leaf, segmentDocID);
        if (isTombstone && fields.uid() == null) {
            op = new Translog.NoOp(seqNo, primaryTerm, ""); // TODO: store reason in ignored fields?
            assert readNumericDV(leaf, Lucene.SOFT_DELETE_FIELD, segmentDocID) == 1
                : "Noop operation but soft_deletes field is not set [" + op + "]";
        } else {
            final String id = fields.uid().id();
            final String type = fields.uid().type();
            final Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(id));
            final long version = readNumericDV(leaf, VersionFieldMapper.NAME, segmentDocID);
            if (isTombstone) {
                op = new Translog.Delete(type, id, uid, seqNo, primaryTerm, version, VersionType.INTERNAL);
                assert readNumericDV(leaf, Lucene.SOFT_DELETE_FIELD, segmentDocID) == 1
                    : "Delete operation but soft_deletes field is not set [" + op + "]";
            } else {
                final BytesReference source = fields.source();
                // TODO: pass the latest timestamp from engine.
                final long autoGeneratedIdTimestamp = -1;
                op = new Translog.Index(type, id, seqNo, primaryTerm, version, VersionType.INTERNAL,
                    source.toBytesRef().bytes, fields.routing(), autoGeneratedIdTimestamp);
            }
        }
        return op;
    }

    private boolean isTombstoneOperation(LeafReaderContext leaf, int segmentDocID) throws IOException {
        final NumericDocValues tombstoneDV = leaf.reader().getNumericDocValues(SeqNoFieldMapper.TOMBSTONE_NAME);
        if (tombstoneDV != null && tombstoneDV.advanceExact(segmentDocID)) {
            return tombstoneDV.longValue() == 1;
        }
        return false;
    }

    private long readNumericDV(LeafReaderContext leaf, String field, int segmentDocID) throws IOException {
        final NumericDocValues dv = leaf.reader().getNumericDocValues(field);
        if (dv == null || dv.advanceExact(segmentDocID) == false) {
            throw new IllegalStateException("DocValues for field [" + field + "] is not found");
        }
        return dv.longValue();
    }
}
