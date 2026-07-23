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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.sourcebatch.LuceneColumn;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

/**
 * The single per-batch context metadata mappers read and write during columnar batch mapping (see
 * {@link ShardBatchMapper}). Deliberately flat: unlike the row-major path's
 * {@link BatchDocumentParserContext}, there is no per-document parser context or {@link LuceneDocument}
 * here — a columnar mapper is invoked once for the whole batch, reads the per-document values it
 * needs from the typed accessor arrays (e.g. {@link #uids()}, {@link #sources()}), and attaches one
 * {@link LuceneColumn} spanning every document via {@link #addColumn}.
 */
public final class BatchMappingContext {

    private final int docCount;
    private final MappingLookup mappingLookup;
    private final IndexSettings indexSettings;
    private final List<LuceneColumn> columns = new ArrayList<>();
    private final FieldNamesFieldMapper fieldNamesFieldMapper;

    // Will go in translog
    /** {@code _seq_no}: docCount * 8 bytes, little-endian longs; lazily allocated. */
    private byte[] seqNo;
    /** {@code _primary_term}: docCount * 8 bytes, little-endian longs; lazily allocated. */
    private byte[] primaryTerm;
    /** {@code _version}: docCount * 8 bytes, little-endian longs; lazily allocated. */
    private byte[] version;
    private final BytesRef[] uids;
    private final BytesRef[] routings;
    private final XContentType[] contentTypes;
    private final BytesReference[] sources;

    private final boolean hasNullUid;

    private boolean frozen;
    /** Per-document {@code _field_names} entries; lazily allocated on first write via {@link #fieldNames()}. */
    private BytesRef[] fieldNames;

    public BatchMappingContext(IndexRequest[] requests, MappingLookup mappingLookup, IndexSettings indexSettings) {
        this.docCount = requests.length;
        this.mappingLookup = mappingLookup;
        this.indexSettings = indexSettings;
        this.fieldNamesFieldMapper = mappingLookup.getMapping().fieldNamesFieldMapper();
        this.uids = new BytesRef[docCount];
        boolean nullUid = false;
        for (int d = 0; d < docCount; d++) {
            final String id = requests[d].id();
            if (id == null) {
                nullUid = true;
            } else {
                this.uids[d] = Uid.encodeId(id);
            }
        }
        this.hasNullUid = nullUid;
        BytesRef[] routingsArr = null;
        for (int d = 0; d < docCount; d++) {
            final String routing = requests[d].routing();
            if (routing != null) {
                if (routingsArr == null) {
                    routingsArr = new BytesRef[docCount];
                }
                routingsArr[d] = new BytesRef(routing);
            }
        }
        this.routings = routingsArr;
        this.contentTypes = new XContentType[docCount];
        this.sources = new BytesReference[docCount];
        for (int d = 0; d < docCount; d++) {
            final XContentType ct = requests[d].getContentType();
            this.contentTypes[d] = ct != null ? ct : XContentType.JSON;
            this.sources[d] = requests[d].source();
        }
    }

    /**
     * Metadata-only constructor used by {@link ShardBatchMapper#buildMetadataContext}. The
     * resulting context has no source data and no mapping lookup; callers must only invoke methods
     * that do not access those (i.e. the metadata mapper columnar-parse hooks). The {@code uids}
     * array is pre-populated so that {@link ProvidedIdFieldMapper} can attach its {@code _id}
     * column.
     */
    BatchMappingContext(int docCount, IndexSettings indexSettings, BytesRef[] uids) {
        this.docCount = docCount;
        this.mappingLookup = null;
        this.indexSettings = indexSettings;
        this.fieldNamesFieldMapper = null;
        this.uids = uids;
        this.routings = null;
        this.hasNullUid = false;
        this.contentTypes = null;
        this.sources = null;
    }

    public MappingLookup mappingLookup() {
        return mappingLookup;
    }

    public IndexSettings indexSettings() {
        return indexSettings;
    }

    /** Attaches a fully-assembled {@link LuceneColumn} covering all {@code docCount} rows. */
    public void addColumn(LuceneColumn column) {
        assert frozen == false;
        columns.add(column);
    }

    /**
     * Returns the {@code _field_names} backing array, or {@code null} if no field names have been
     * registered for any document in the batch. Called only by {@link FieldNamesFieldMapper} during
     * {@link FieldNamesFieldMapper#postColumnarParse}.
     */
    @Nullable
    BytesRef[] fieldNamesIfPresent() {
        return fieldNames;
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
     * Returns the routing array, or {@code null} if no document in the chunk has an explicit
     * routing (the common case). When non-null, individual entries may still be {@code null} for
     * documents without routing.
     */
    public BytesRef[] routings() {
        return routings;
    }

    /** Returns the per-document content-type array; entries default to {@link XContentType#JSON} when the request had none. */
    public XContentType[] contentTypes() {
        return contentTypes;
    }

    /**
     * Returns the per-document source array. Individual entries may be {@code null} for documents
     * that carry no source.
     */
    public BytesReference[] sources() {
        return sources;
    }

    /**
     * Returns the {@code _id} (Uid-encoded) array.
     *
     * @throws IllegalStateException if any document in the batch has a null {@code _id} (synthetic
     *     id is not yet supported in the columnar path)
     */
    public BytesRef[] uids() {
        if (hasNullUid) {
            // TODO: We do not support synthetic id yet. This will change once we do.
            throw new IllegalStateException("_id should have been set on the coordinating node");
        }
        return uids;
    }

    /**
     * Records that {@code field} should appear in {@code _field_names} for document {@code doc}.
     * Delegates to {@link FieldNamesFieldMapper} which owns the per-document accumulation and
     * column assembly. No-op when {@code _field_names} is absent or disabled for the index.
     */
    public void addFieldNamesColumnar(int doc, String field) {
        assert frozen == false;
        if (fieldNamesFieldMapper != null) {
            fieldNamesFieldMapper.addFieldNamesColumnar(this, doc, field);
        }
    }

    /**
     * Lazily allocates and returns the {@code _field_names} backing array (length {@code docCount}).
     * Called only by {@link FieldNamesFieldMapper} when registering a field name.
     */
    BytesRef[] fieldNames() {
        if (fieldNames == null) {
            // TODO: Single value only currently. Will replace this with a multi-value Escf array column.
            fieldNames = new BytesRef[docCount];
        }
        return fieldNames;
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
        frozen = true;
        return new MappedColumns(0, docCount, seqNo, primaryTerm, version, columns);
    }
}
