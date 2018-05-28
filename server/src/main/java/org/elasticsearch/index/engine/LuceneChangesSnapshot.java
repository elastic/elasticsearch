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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
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
import java.util.Objects;

/**
 * A {@link Translog.Snapshot} from changes in a Lucene index
 */
final class LuceneChangesSnapshot implements Translog.Snapshot {
    private final long fromSeqNo, toSeqNo;
    private long lastSeenSeqNo;
    private int skippedOperations;
    private final boolean requiredFullRange;

    private final IndexSearcher indexSearcher;
    private final MapperService mapperService;
    private int docIndex = 0;
    private final TopDocs topDocs;

    private final Closeable onClose;
    private final CombinedDocValues[] docValues; // Cache of DocValues

    /**
     * Creates a new "translog" snapshot from Lucene for reading operations whose seq# in the specified range.
     *
     * @param engineSearcher    the internal engine searcher which will be taken over if the snapshot is opened successfully
     * @param mapperService     the mapper service which will be mainly used to resolve the document's type and uid
     * @param fromSeqNo         the min requesting seq# - inclusive
     * @param toSeqNo           the maximum requesting seq# - inclusive
     * @param requiredFullRange if true, the snapshot will strictly check for the existence of operations between fromSeqNo and toSeqNo
     */
    LuceneChangesSnapshot(Engine.Searcher engineSearcher, MapperService mapperService,
                          long fromSeqNo, long toSeqNo, boolean requiredFullRange) throws IOException {
        if (fromSeqNo < 0 || toSeqNo < 0 || fromSeqNo > toSeqNo) {
            throw new IllegalArgumentException("Invalid range; from_seqno [" + fromSeqNo + "], to_seqno [" + toSeqNo + "]");
        }
        this.mapperService = mapperService;
        this.fromSeqNo = fromSeqNo;
        this.toSeqNo = toSeqNo;
        this.lastSeenSeqNo = fromSeqNo - 1;
        this.requiredFullRange = requiredFullRange;
        this.indexSearcher = new IndexSearcher(Lucene.wrapAllDocsLive(engineSearcher.getDirectoryReader()));
        this.indexSearcher.setQueryCache(null);
        this.topDocs = searchOperations(indexSearcher);
        final List<LeafReaderContext> leaves = indexSearcher.getIndexReader().leaves();
        this.docValues = new CombinedDocValues[leaves.size()];
        for (LeafReaderContext leaf : leaves) {
            this.docValues[leaf.ord] = new CombinedDocValues(leaf.reader());
        }
        this.onClose = engineSearcher;
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
        Translog.Operation op = null;
        for (int docId = nextDocId(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = nextDocId()) {
            op = readDocAsOp(docId);
            if (op != null) {
                break;
            }
        }
        if (requiredFullRange) {
            rangeCheck(op);
        }
        if (op != null) {
            lastSeenSeqNo = op.seqNo();
        }
        return op;
    }

    private void rangeCheck(Translog.Operation op) {
        if (op == null) {
            if (lastSeenSeqNo < toSeqNo) {
                throw new IllegalStateException("Not all operations between min_seqno [" + fromSeqNo + "] " +
                    "and max_seqno [" + toSeqNo + "] found; prematurely terminated last_seen_seqno [" + lastSeenSeqNo + "]");
            }
        } else {
            final long expectedSeqNo = lastSeenSeqNo + 1;
            if (op.seqNo() != expectedSeqNo) {
                throw new IllegalStateException("Not all operations between min_seqno [" + fromSeqNo + "] " +
                    "and max_seqno [" + toSeqNo + "] found; expected seqno [" + expectedSeqNo + "]; found [" + op + "]");
            }
        }
    }

    private int nextDocId() {
        if (docIndex < topDocs.scoreDocs.length) {
            final int docId = topDocs.scoreDocs[docIndex].doc;
            docIndex++;
            return docId;
        } else {
            return DocIdSetIterator.NO_MORE_DOCS;
        }
    }

    private TopDocs searchOperations(IndexSearcher searcher) throws IOException {
        final Query rangeQuery = new BooleanQuery.Builder()
            .add(new DocValuesFieldExistsQuery(SeqNoFieldMapper.PRIMARY_TERM_NAME), BooleanClause.Occur.FILTER)
            .add(LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, fromSeqNo, toSeqNo), BooleanClause.Occur.FILTER)
            .build();
        final Sort sortedBySeqNoThenByTerm = new Sort(
            new SortedNumericSortField(SeqNoFieldMapper.NAME, SortField.Type.LONG),
            new SortedNumericSortField(SeqNoFieldMapper.PRIMARY_TERM_NAME, SortField.Type.LONG, true)
        );
        // norelease - limits the number of hits
        final long numHits = Math.min((toSeqNo + 1 - fromSeqNo) * 2, Integer.MAX_VALUE - 1);
        return searcher.search(rangeQuery, Math.toIntExact(numHits), sortedBySeqNoThenByTerm);
    }

    private Translog.Operation readDocAsOp(int docID) throws IOException {
        final List<LeafReaderContext> leaves = indexSearcher.getIndexReader().leaves();
        final LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(docID, leaves));
        final int segmentDocID = docID - leaf.docBase;
        final long primaryTerm = docValues[leaf.ord].docPrimaryTerm(segmentDocID);
        // We don't have to read the nested child documents - those docs don't have primary terms.
        if (primaryTerm == -1) {
            skippedOperations++;
            return null;
        }
        final long seqNo = docValues[leaf.ord].docSeqNo(segmentDocID);
        // Only pick the first seen seq#
        if (seqNo == lastSeenSeqNo) {
            skippedOperations++;
            return null;
        }
        final long version = docValues[leaf.ord].docVersion(segmentDocID);
        final FieldsVisitor fields = new FieldsVisitor(true);
        indexSearcher.doc(docID, fields);
        fields.postProcess(mapperService);

        final Translog.Operation op;
        final boolean isTombstone = docValues[leaf.ord].isTombstone(segmentDocID);
        if (isTombstone && fields.uid() == null) {
            op = new Translog.NoOp(seqNo, primaryTerm, fields.source().utf8ToString());
            assert version == 1L : "Noop tombstone should have version 1L; actual version [" + version + "]";
            assert assertDocSoftDeleted(leaf.reader(), segmentDocID) : "Noop but soft_deletes field is not set [" + op + "]";
        } else {
            final String id = fields.uid().id();
            final String type = fields.uid().type();
            final Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(id));
            if (isTombstone) {
                op = new Translog.Delete(type, id, uid, seqNo, primaryTerm, version, VersionType.INTERNAL);
                assert assertDocSoftDeleted(leaf.reader(), segmentDocID) : "Delete op but soft_deletes field is not set [" + op + "]";
            } else {
                final BytesReference source = fields.source();
                // TODO: pass the latest timestamp from engine.
                final long autoGeneratedIdTimestamp = -1;
                op = new Translog.Index(type, id, seqNo, primaryTerm, version, VersionType.INTERNAL,
                    source.toBytesRef().bytes, fields.routing(), autoGeneratedIdTimestamp);
            }
        }
        assert fromSeqNo <= op.seqNo() && op.seqNo() <= toSeqNo && lastSeenSeqNo < op.seqNo() : "Unexpected operation; " +
            "last_seen_seqno [" + lastSeenSeqNo + "], from_seqno [" + fromSeqNo + "], to_seqno [" + toSeqNo + "], op [" + op + "]";
        return op;
    }

    private boolean assertDocSoftDeleted(LeafReader leafReader, int segmentDocId) throws IOException {
        final NumericDocValues ndv = leafReader.getNumericDocValues(Lucene.SOFT_DELETE_FIELD);
        if (ndv == null || ndv.advanceExact(segmentDocId) == false) {
            throw new IllegalStateException("DocValues for field [" + Lucene.SOFT_DELETE_FIELD + "] is not found");
        }
        return ndv.longValue() == 1;
    }

    private static final class CombinedDocValues {
        private final LeafReader leafReader;
        private NumericDocValues versionDV;
        private NumericDocValues seqNoDV;
        private NumericDocValues primaryTermDV;
        private NumericDocValues tombstoneDV;

        CombinedDocValues(LeafReader leafReader) throws IOException {
            this.leafReader = leafReader;
            this.versionDV = Objects.requireNonNull(leafReader.getNumericDocValues(VersionFieldMapper.NAME), "VersionDV is missing");
            this.seqNoDV = Objects.requireNonNull(leafReader.getNumericDocValues(SeqNoFieldMapper.NAME), "SeqNoDV is missing");
            this.primaryTermDV = Objects.requireNonNull(
                leafReader.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME), "PrimaryTermDV is missing");
            this.tombstoneDV = leafReader.getNumericDocValues(SeqNoFieldMapper.TOMBSTONE_NAME);
        }

        long docVersion(int segmentDocId) throws IOException {
            if (versionDV.docID() > segmentDocId) {
                versionDV = Objects.requireNonNull(leafReader.getNumericDocValues(VersionFieldMapper.NAME), "VersionDV is missing");
            }
            if (versionDV.advanceExact(segmentDocId) == false) {
                throw new IllegalStateException("DocValues for field [" + VersionFieldMapper.NAME + "] is not found");
            }
            return versionDV.longValue();
        }

        long docSeqNo(int segmentDocId) throws IOException {
            if (seqNoDV.docID() > segmentDocId) {
                seqNoDV = Objects.requireNonNull(leafReader.getNumericDocValues(SeqNoFieldMapper.NAME), "SeqNoDV is missing");
            }
            if (seqNoDV.advanceExact(segmentDocId) == false) {
                throw new IllegalStateException("DocValues for field [" + SeqNoFieldMapper.NAME + "] is not found");
            }
            return seqNoDV.longValue();
        }

        long docPrimaryTerm(int segmentDocId) throws IOException {
            if (primaryTermDV == null) {
                return -1L;
            }
            if (primaryTermDV.docID() > segmentDocId) {
                primaryTermDV = leafReader.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
            }
            // Use -1 for docs which don't have primary term. The caller considers those docs as nested docs.
            if (primaryTermDV.advanceExact(segmentDocId) == false) {
                return -1;
            }
            return primaryTermDV.longValue();
        }

        boolean isTombstone(int segmentDocId) throws IOException {
            if (tombstoneDV == null) {
                return false;
            }
            if (tombstoneDV.docID() > segmentDocId) {
                tombstoneDV = leafReader.getNumericDocValues(SeqNoFieldMapper.TOMBSTONE_NAME);
            }
            return tombstoneDV.advanceExact(segmentDocId) && tombstoneDV.longValue() > 0;
        }
    }
}
