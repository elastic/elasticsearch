/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SliceableColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The single per-batch context metadata mappers read and write during columnar batch mapping (see
 * {@link ShardBatchMapper}). Deliberately flat: unlike the row-major path's
 * {@link BatchDocumentParserContext}, there is no per-document parser context or {@link LuceneDocument}
 * here — a columnar mapper is invoked once for the whole batch, reads the per-document
 * values it needs straight off the chunk-local {@link IndexRequest}s, and attaches one
 * {@link SliceableColumn} spanning every document via {@link #addColumn}.
 */
public final class BatchMappingContext {

    // TODO: Need to remove dependency on the IndexRequest object. We currently need it for source and tsid.
    private final IndexRequest[] requests;
    private final int docCount;
    private final MappingLookup mappingLookup;
    private final IndexSettings indexSettings;
    private final List<SliceableColumn> columns = new ArrayList<>();
    private final FieldNamesFieldMapper fieldNamesFieldMapper;

    // Will go in translog
    /** {@code _seq_no}: docCount * 8 bytes, little-endian longs; lazily allocated. */
    private byte[] seqNo;
    /** {@code _primary_term}: docCount * 8 bytes, little-endian longs; lazily allocated. */
    private byte[] primaryTerm;
    /** {@code _version}: docCount * 8 bytes, little-endian longs; lazily allocated. */
    private byte[] version;
    private BytesRef[] uids;
    private BytesRef[] routings;

    private boolean routingsInitialized;
    /**
     * Accumulates {@code (doc, name)} pairs for {@code _field_names}. Multiple contributors may
     * call {@link #addFieldNamesColumnar} for any doc in any order; {@link FieldNamesFieldMapper}
     * sorts and drains these in {@code postColumnarParse}. Lazily allocated on first write.
     */
    private int[] fieldNameDocs;
    private BytesRef[] fieldNameValues;
    private int fieldNameCount;

    public BatchMappingContext(IndexRequest[] requests, MappingLookup mappingLookup, IndexSettings indexSettings) {
        this.requests = requests;
        this.docCount = requests.length;
        this.mappingLookup = mappingLookup;
        this.indexSettings = indexSettings;
        this.fieldNamesFieldMapper = mappingLookup.getMapping().fieldNamesFieldMapper();
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

    /** Attaches a fully-assembled {@link SliceableColumn} covering all {@code docCount} documents. */
    public void addColumn(SliceableColumn column) {
        columns.add(column);
    }

    /**
     * Returns the number of {@code (doc, name)} entries accumulated so far for {@code _field_names}.
     * Zero when no contributor has called {@link #addFieldNamesColumnar}. Called only by
     * {@link FieldNamesFieldMapper} during {@link FieldNamesFieldMapper#postColumnarParse}.
     */
    int fieldNameCount() {
        return fieldNameCount;
    }

    /**
     * Returns the doc-index parallel array for the {@code _field_names} accumulator.
     * Valid only when {@link #fieldNameCount()} &gt; 0.
     */
    int[] fieldNameDocs() {
        return fieldNameDocs;
    }

    /**
     * Returns the name parallel array for the {@code _field_names} accumulator.
     * Valid only when {@link #fieldNameCount()} &gt; 0.
     */
    BytesRef[] fieldNameValues() {
        return fieldNameValues;
    }

    /**
     * Lazily allocates and returns the mutable {@code _seq_no} backing byte array (length
     * {@code docCount * 8}). Each 8-byte slot is pre-filled with
     * {@link SequenceNumbers#UNASSIGNED_SEQ_NO} in little-endian order; the engine overwrites
     * the real per-document value after mapping.
     */
    public byte[] seqNos() {
        if (seqNo == null) {
            seqNo = new byte[docCount * 8];
            // Fill every 8-byte slot with UNASSIGNED_SEQ_NO; Arrays.fill cannot be used because
            // a long value is not a repeated byte pattern.
            for (int d = 0; d < docCount; d++) {
                ByteUtils.writeLongLE(SequenceNumbers.UNASSIGNED_SEQ_NO, seqNo, d * 8);
            }
        }
        return seqNo;
    }

    /**
     * Lazily allocates and returns the mutable {@code _primary_term} backing byte array (length
     * {@code docCount * 8}). Slots are zero-initialized (0L default); the engine fills the real
     * value after mapping.
     */
    public byte[] primaryTerms() {
        if (primaryTerm == null) {
            primaryTerm = new byte[docCount * 8];
        }
        return primaryTerm;
    }

    /**
     * Lazily allocates and returns the mutable {@code _version} backing byte array (length
     * {@code docCount * 8}). Slots are zero-initialized (0L default); the engine fills the real
     * value after mapping.
     */
    public byte[] versions() {
        if (version == null) {
            version = new byte[docCount * 8];
        }
        return version;
    }

    /**
     * Lazily computes and returns the routing array, or {@code null} if no document in the chunk
     * has an explicit routing (the common case). When non-null, individual entries may still be
     * {@code null} for documents without routing.
     */
    public BytesRef[] routings() {
        if (routingsInitialized == false) {
            for (int d = 0; d < docCount; d++) {
                final String routing = requests[d].routing();
                if (routing != null) {
                    if (routings == null) {
                        routings = new BytesRef[docCount];
                    }
                    routings[d] = new BytesRef(routing);
                }
            }
            routingsInitialized = true;
        }
        return routings;
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

    /**
     * Records that {@code field} should appear in {@code _field_names} for document {@code doc}.
     * Delegates to {@link FieldNamesFieldMapper} which owns the per-document accumulation and
     * column assembly. No-op when {@code _field_names} is absent or disabled for the index.
     */
    public void addFieldNamesColumnar(int doc, String field) {
        if (fieldNamesFieldMapper != null) {
            fieldNamesFieldMapper.addFieldNamesColumnar(this, doc, field);
        }
    }

    /**
     * Appends a {@code (doc, value)} pair to the {@code _field_names} accumulator. Called only by
     * {@link FieldNamesFieldMapper#addFieldNamesColumnar}; the arrays are grown as needed and drained
     * in sorted order by {@link FieldNamesFieldMapper#postColumnarParse}.
     */
    void recordFieldName(int doc, BytesRef value) {
        if (fieldNameDocs == null) {
            fieldNameDocs = new int[4];
            fieldNameValues = new BytesRef[4];
        } else if (fieldNameCount == fieldNameDocs.length) {
            fieldNameDocs = Arrays.copyOf(fieldNameDocs, fieldNameDocs.length * 2);
            fieldNameValues = Arrays.copyOf(fieldNameValues, fieldNameValues.length * 2);
        }
        fieldNameDocs[fieldNameCount] = doc;
        fieldNameValues[fieldNameCount] = value;
        fieldNameCount++;
    }

    /** The number of documents in this chunk. */
    public int docCount() {
        return docCount;
    }

    /**
     * Returns the accumulated columns as a {@link MappedColumns} covering the full batch
     * {@code [0, docCount)}. The engine slices this per sub-batch before calling
     * {@link MappedColumns#toColumnBatch()}.
     */
    public MappedColumns columns() {
        return new MappedColumns(0, docCount, seqNo, primaryTerm, version, columns);
    }
}
