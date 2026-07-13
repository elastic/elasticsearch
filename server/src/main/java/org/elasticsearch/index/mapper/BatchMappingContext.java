/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.sourcebatch.ColumnBatchProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The single per-batch context metadata mappers read and write during columnar batch mapping (see
 * {@link ShardBatchMapper}). Deliberately flat: unlike the row-major path's
 * {@link BatchDocumentParserContext}, there is no per-document parser context or {@link LuceneDocument}
 * here — a columnar metadata mapper is invoked once for the whole batch, reads the per-document
 * values it needs straight off the chunk-local {@link IndexRequest}s, and attaches one Lucene
 * {@link Column} spanning every document via {@link #addColumn}.
 *
 * <p>Implements {@link ColumnBatchProvider} so the engine can fill the {@code _seq_no}/
 * {@code _primary_term}/{@code _version} columns (registered by their mappers as array-backed
 * placeholders) after mapping, then request the assembled {@link ColumnBatch}.
 */
public final class BatchMappingContext implements ColumnBatchProvider {

    // TODO: Need to remove dependency on the IndexRequest object. We currently need it for source and tsid.
    private final IndexRequest[] requests;
    private final int docCount;
    private final MappingLookup mappingLookup;
    private final IndexSettings indexSettings;
    private final List<Column> columns = new ArrayList<>();

    // Will go in translog
    private long[] seqNo;
    private long[] primaryTerm;
    private long[] version;
    private BytesRef[] uids;

    // Not stored in translog

    public BatchMappingContext(IndexRequest[] requests, MappingLookup mappingLookup, IndexSettings indexSettings) {
        this.requests = requests;
        this.docCount = requests.length;
        this.mappingLookup = mappingLookup;
        this.indexSettings = indexSettings;
    }

    public MappingLookup mappingLookup() {
        return mappingLookup;
    }

    public IndexSettings indexSettings() {
        return indexSettings;
    }

    /** The chunk-local index request for document {@code doc}. */
    public IndexRequest request(int doc) {
        return requests[doc];
    }

    /** Convenience accessor for {@code request(doc).id()}. */
    public String id(int doc) {
        return requests[doc].id();
    }

    /** Convenience accessor for {@code request(doc).routing()}. */
    public String routing(int doc) {
        return requests[doc].routing();
    }

    /** Attaches a fully-assembled Lucene column covering all {@code docCount} documents. */
    public void addColumn(Column column) {
        columns.add(column);
    }

    /** Lazily allocates and returns the mutable {@code _seq_no} backing array (length {@code docCount}). */
    public long[] seqNos() {
        if (seqNo == null) {
            seqNo = new long[docCount];
            Arrays.fill(seqNo, SequenceNumbers.UNASSIGNED_SEQ_NO);
        }
        return seqNo;
    }

    /** Lazily allocates and returns the mutable {@code _primary_term} backing array (length {@code docCount}). */
    public long[] primaryTerms() {
        if (primaryTerm == null) {
            primaryTerm = new long[docCount];
        }
        return primaryTerm;
    }

    /** Lazily allocates and returns the mutable {@code _version} backing array (length {@code docCount}). */
    public long[] versions() {
        if (version == null) {
            version = new long[docCount];
        }
        return version;
    }

    /**
     * Lazily computes and returns the {@code _id} (Uid-encoded) array.
     */
    public BytesRef[] uids() {
        if (uids == null) {
            uids = new BytesRef[docCount];
            for (int d = 0; d < docCount; d++) {
                final String id = requests[d].id();
                if (id == null) {
                    throw new IllegalStateException("_id should have been set on the coordinating node");
                }
                uids[d] = Uid.encodeId(id);
            }
        }
        return uids;
    }

    @Override
    public int docCount() {
        return docCount;
    }

    @Override
    public void setSeqNo(int doc, long value) {
        seqNos()[doc] = value;
    }

    @Override
    public void fillPrimaryTerm(long value) {
        Arrays.fill(primaryTerms(), value);
    }

    @Override
    public void setVersion(int doc, long value) {
        versions()[doc] = value;
    }

    @Override
    public ColumnBatch columnBatch(int from, int to) {
        if (from != 0 || to != docCount) {
            // First cut: a chunk is indexed atomically as one addBatch; sub-range slicing is a follow-up.
            throw new UnsupportedOperationException(
                "BatchMappingContext only supports the full range [0, " + docCount + "), got [" + from + ", " + to + ")"
            );
        }
        final List<Column> batchColumns = List.copyOf(columns);
        return new LuceneColumnBatch(batchColumns, docCount);
    }

    private static class LuceneColumnBatch extends ColumnBatch {
        private final List<Column> batchColumns;
        private final int docCount;

        private LuceneColumnBatch(List<Column> batchColumns, int docCount) {
            this.batchColumns = batchColumns;
            this.docCount = docCount;
        }

        @Override
        public int numDocs() {
            return docCount;
        }

        @Override
        public Iterable<Column> columns() {
            return batchColumns;
        }
    }
}
