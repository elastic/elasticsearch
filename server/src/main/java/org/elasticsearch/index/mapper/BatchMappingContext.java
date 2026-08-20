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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.EscfLongColumn;
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
    private final Recycler<BytesRef> recycler;
    private final List<LuceneColumn> columns = new ArrayList<>();
    private final FieldNamesFieldMapper fieldNamesFieldMapper;

    private boolean frozen;
    /** Accumulates {@code (doc, name)} pairs for {@code _field_names}. */
    private DeduplicatingStringColumnAccumulator fieldNames;
    /** Accumulates {@code (doc, name)} pairs for {@code _ignored}. */
    private DeduplicatingStringColumnAccumulator ignoredFields;
    /**
     * The mapped {@code @timestamp} column, published by {@code DateFieldMapper.mapColumnBatch}
     * when it maps the data-stream timestamp field. Readable via {@link #timestamps()}, and will be
     * {@code null} before the column is mapped. Mirrors the per-document
     * side channel that {@link DataStreamTimestampFieldMapper} uses on the row path
     * ({@code DataStreamTimestampFieldMapper.storeTimestampValueForReuse}).
     */
    private EscfLongColumn timestamps;

    /**
     * Primary constructor. Delegates all per-doc data accessors to {@code batch} and records
     * accumulated columns and field names during mapping.
     */
    public BatchMappingContext(
        IndexOperationBatch batch,
        MappingLookup mappingLookup,
        IndexSettings indexSettings,
        Recycler<BytesRef> recycler
    ) {
        this.batch = batch;
        this.mappingLookup = mappingLookup;
        this.indexSettings = indexSettings;
        this.recycler = recycler;
        this.fieldNamesFieldMapper = mappingLookup.getMapping().fieldNamesFieldMapper();
    }

    public IndexSettings indexSettings() {
        return indexSettings;
    }

    /** Returns the mapping lookup for this batch's index, for inspecting mapper configuration during post-parse. */
    public MappingLookup mappingLookup() {
        return mappingLookup;
    }

    /**
     * Records the mapped {@code @timestamp} ESCF column so that {@code postColumnarParse} hooks
     * can read per-document timestamp values via {@link #timestamps()} without re-scanning
     * the Lucene column list. Mirrors the row-path side channel
     * ({@code DataStreamTimestampFieldMapper.storeTimestampValueForReuse}). The data is the same
     * {@link EscfColumnData} that {@code DateFieldMapper.mapColumnBatch} already built; no copy
     * is made. Multivalued (array) timestamp columns are rejected the same way the row path
     * rejects multiple values for the same document.
     *
     * @throws IllegalArgumentException if called more than once or if the column is multivalued
     */
    public void setTimestamps(EscfColumnData timestamps) {
        if (this.timestamps != null) {
            throw new IllegalArgumentException(
                "data stream timestamp field [" + DataStreamTimestampFieldMapper.DEFAULT_PATH + "] encountered multiple values"
            );
        }
        if (timestamps.kind() == EscfColumnKind.ARRAY) {
            throw new IllegalArgumentException(
                "data stream timestamp field [" + DataStreamTimestampFieldMapper.DEFAULT_PATH + "] encountered multiple values"
            );
        }
        this.timestamps = (EscfLongColumn) EscfColumn.from(timestamps);
    }

    /**
     * Returns the mapped {@code @timestamp} column, or {@code null} if no column has been recorded
     * yet. Callers are responsible for density and size validation before iterating values.
     */
    public EscfLongColumn timestamps() {
        return timestamps;
    }

    /**
     * Whether {@code _data_stream_timestamp} is present and enabled for this index. Mirrors
     * {@link MappingLookup#isDataStreamTimestampFieldEnabled()} which is the row-path equivalent
     * used by {@code DateFieldMapper.indexValue}.
     */
    public boolean isDataStreamTimestampFieldEnabled() {
        return mappingLookup.isDataStreamTimestampFieldEnabled();
    }

    // TODO: nothing allocates through this yet — the columns it would produce have no owner to release them.
    public Recycler<BytesRef> recycler() {
        return recycler;
    }

    /** Attaches a fully-assembled {@link LuceneColumn} covering all {@code docCount} rows. */
    public void addColumn(LuceneColumn column) {
        assert frozen == false;
        columns.add(column);
    }

    /**
     * Returns the {@code _field_names} accumulator, or {@code null} if no entry has been recorded
     * yet. Called only by {@link FieldNamesFieldMapper} during
     * {@link FieldNamesFieldMapper#postColumnarParse}.
     */
    DeduplicatingStringColumnAccumulator fieldNamesAccumulator() {
        return fieldNames;
    }

    /**
     * Returns the mutable {@code _seq_no} buffer. Delegated to the underlying
     * {@link IndexOperationBatch#seqNoBytes()}; see that method for the aliasing contract.
     */
    public BytesRef seqNos() {
        return batch.seqNoBytes();
    }

    /**
     * Returns the mutable {@code _primary_term} buffer. Delegated to the underlying
     * {@link IndexOperationBatch#primaryTermBytes()}.
     */
    public BytesRef primaryTerms() {
        return batch.primaryTermBytes();
    }

    /**
     * Returns the mutable {@code _version} buffer. Delegated to the underlying
     * {@link IndexOperationBatch#versionBytes()}.
     */
    public BytesRef versions() {
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
     * Records a {@code (doc, value)} pair in the {@code _field_names} accumulator. Called only by
     * {@link FieldNamesFieldMapper#addFieldNamesColumnar}; drained by
     * {@link FieldNamesFieldMapper#postColumnarParse}.
     */
    void recordFieldName(int doc, BytesRef value) {
        if (fieldNames == null) {
            fieldNames = new DeduplicatingStringColumnAccumulator(batch.docCount());
        }
        fieldNames.record(doc, value);
    }

    /**
     * Returns the {@code _ignored} accumulator, or {@code null} if no entry has been recorded yet.
     * Called only by {@link IgnoredFieldMapper} during
     * {@link IgnoredFieldMapper#postColumnarParse}.
     */
    DeduplicatingStringColumnAccumulator ignoredFieldsAccumulator() {
        return ignoredFields;
    }

    /**
     * Records that {@code field} was ignored for document {@code doc} (e.g. a keyword value that
     * tripped {@code ignore_above}), to be emitted in {@code _ignored}. Unlike the row-major path —
     * where {@link DocumentParserContext#addIgnoredField} is called once per value and de-duplicated
     * through a {@link java.util.Set} — a columnar field mapper is invoked once per batch and records
     * a single per-document decision, so each {@code (doc, field)} pair is unique. The accumulator is
     * drained by {@link IgnoredFieldMapper#postColumnarParse}.
     */
    public void addIgnoredFieldColumnar(int doc, String field) {
        assert frozen == false;
        if (ignoredFields == null) {
            ignoredFields = new DeduplicatingStringColumnAccumulator(batch.docCount());
        }
        ignoredFields.record(doc, new BytesRef(field));
    }

    /**
     * Whether {@code _source} is reconstructed from doc values.
     */
    public boolean isSourceSynthetic() {
        return mappingLookup.isSourceSynthetic();
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
