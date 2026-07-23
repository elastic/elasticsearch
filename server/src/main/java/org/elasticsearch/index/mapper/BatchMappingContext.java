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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.IndexOperationBatch;
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
 *
 * <p>Per-document data (uids, routings, content types, sources, and the engine-written
 * seqNo/primaryTerm/version byte arrays) is owned by the underlying {@link IndexOperationBatch}.
 * This context is a read-only view over that batch; it additionally accumulates the mapping-time
 * state (the assembled {@link LuceneColumn} list and the {@code _field_names} entries) that belongs
 * to the mapping phase rather than the operation record.
 */
public final class BatchMappingContext {

    private final IndexOperationBatch batch;
    private final MappingLookup mappingLookup;
    private final IndexSettings indexSettings;
    private final List<LuceneColumn> columns = new ArrayList<>();
    private final FieldNamesFieldMapper fieldNamesFieldMapper;

    private boolean frozen;
    /** Per-document {@code _field_names} entries; lazily allocated on first write via {@link #fieldNames()}. */
    private BytesRef[] fieldNames;

    /**
     * Primary constructor. Delegates all per-doc data accessors to {@code batch} and records
     * accumulated columns and field names during mapping.
     */
    public BatchMappingContext(IndexOperationBatch batch, MappingLookup mappingLookup, IndexSettings indexSettings) {
        this.batch = batch;
        this.mappingLookup = mappingLookup;
        this.indexSettings = indexSettings;
        this.fieldNamesFieldMapper = mappingLookup.getMapping().fieldNamesFieldMapper();
    }

    /**
     * Metadata-only constructor used by {@link ShardBatchMapper#buildMetadataContext}. The
     * resulting context has no mapping lookup or field-names mapper; callers must only invoke methods
     * that do not access those (i.e. the metadata mapper columnar-parse hooks). The underlying
     * {@code batch} should be a {@link IndexOperationBatch#metadataOnly(int, BytesRef[])} instance
     * so that {@link ProvidedIdFieldMapper} can attach its {@code _id} column.
     */
    BatchMappingContext(IndexOperationBatch batch, IndexSettings indexSettings) {
        this.batch = batch;
        this.mappingLookup = null;
        this.indexSettings = indexSettings;
        this.fieldNamesFieldMapper = null;
    }

    /**
     * Convenience factory for mapper unit tests that exercise the columnar metadata-mapper hooks
     * directly against a set of {@link IndexRequest}s. Equivalent to:
     * <pre>
     *   new BatchMappingContext(IndexOperationBatch.initFromRequests(requests), mappingLookup, indexSettings)
     * </pre>
     */
    public static BatchMappingContext fromRequests(IndexRequest[] requests, MappingLookup mappingLookup, IndexSettings indexSettings) {
        return new BatchMappingContext(IndexOperationBatch.initFromRequests(requests), mappingLookup, indexSettings);
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
     * Returns the mutable {@code _seq_no} backing byte array. Delegated to the underlying
     * {@link IndexOperationBatch#seqNoBytes()}; see that method for the aliasing contract.
     */
    public byte[] seqNos() {
        return batch.seqNoBytes();
    }

    /**
     * Returns the mutable {@code _primary_term} backing byte array. Delegated to the underlying
     * {@link IndexOperationBatch#primaryTermBytes()}.
     */
    public byte[] primaryTerms() {
        return batch.primaryTermBytes();
    }

    /**
     * Returns the mutable {@code _version} backing byte array. Delegated to the underlying
     * {@link IndexOperationBatch#versionBytes()}.
     */
    public byte[] versions() {
        return batch.versionBytes();
    }

    /**
     * Returns the routing array, or {@code null} if no document in the chunk has an explicit
     * routing (the common case). When non-null, individual entries may still be {@code null} for
     * documents without routing.
     */
    public BytesRef[] routings() {
        return batch.routings();
    }

    /** Returns the per-document content-type array; entries default to {@link XContentType#JSON} when the request had none. */
    public XContentType[] contentTypes() {
        return batch.contentTypes();
    }

    /**
     * Returns the per-document source array. Individual entries may be {@code null} for documents
     * that carry no source.
     */
    public BytesReference[] sources() {
        return batch.sources();
    }

    /**
     * Returns the {@code _id} (Uid-encoded) array.
     *
     * @throws IllegalStateException if any document in the batch has a null {@code _id} (synthetic
     *     id is not yet supported in the columnar path)
     */
    public BytesRef[] uids() {
        return batch.uids();
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
            fieldNames = new BytesRef[batch.docCount()];
        }
        return fieldNames;
    }

    /** The number of documents in this chunk. */
    public int docCount() {
        return batch.docCount();
    }

    /**
     * Returns the accumulated columns as a {@link MappedColumns} covering the full batch
     * {@code [0, docCount)}. The engine slices this per sub-batch before calling
     * {@link MappedColumns#toColumnBatch()}.
     *
     * <p>The seqNo, primaryTerm, and version byte arrays are aliased by reference from the
     * underlying {@link IndexOperationBatch}, so engine writes through
     * {@link MappedColumns#setSeqNo}/{@link MappedColumns#fillPrimaryTerm}/
     * {@link MappedColumns#setVersion} are immediately visible to the Lucene columns.
     */
    public MappedColumns columns() {
        frozen = true;
        return new MappedColumns(0, batch.docCount(), batch.seqNoBytes(), batch.primaryTermBytes(), batch.versionBytes(), columns);
    }
}
