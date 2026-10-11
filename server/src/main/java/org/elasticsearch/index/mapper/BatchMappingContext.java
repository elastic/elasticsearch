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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.escf.EscfLongColumn;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.sourcebatch.LuceneColumn;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

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
public final class BatchMappingContext implements Releasable {

    private final IndexOperationBatch batch;
    private final MappingLookup mappingLookup;
    private final IndexSettings indexSettings;
    private final Recycler<BytesRef> recycler;
    private final List<LuceneColumn> columns = new ArrayList<>();
    private final List<Releasable> resources = new ArrayList<>();
    private final FieldNamesFieldMapper fieldNamesFieldMapper;

    /**
     * The per-field {@code _source} writers registered by data field mappers that support the {@code columnar_stored} direct source
     * path (see {@link FieldMapper#supportsColumnarSource()}). Drained by {@link SourceFieldMapper#postColumnarParse} to build the
     * blob directly, bypassing the per-document synthetic-source loader, but only when {@link #directSourceAvailable()} holds.
     */
    private final List<CompositeSourceColumn> sourceColumns = new ArrayList<>();
    /**
     * Set to {@code true} the moment any source-contributing field of the batch cannot be written directly — because its mapper does
     * not support the direct path, or supports it in general but not for the shapes in this batch. Default-deny: a mapper added later
     * is correct (it falls back to the loader path) without any change here, only slower.
     */
    private boolean directSourceUnavailable;
    /**
     * Set by {@link FieldMapper#mapColumnBatch} around a single {@link FieldMapper#doMapColumnBatch} call, for a source-contributing
     * field whose mapper {@link FieldMapper#supportsColumnarSource() supports} the direct path. It tells that mapper to build its
     * {@link CompositeSourceColumn} during the same scan that maps the field's Lucene columns — registering it via
     * {@link #registerSourceColumn} — instead of re-scanning the source column in a second pass.
     */
    private boolean buildSourceColumnRequested;

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

    /**
     * Records the mapped {@code @timestamp} ESCF column so that {@code postColumnarParse} hooks
     * (e.g. {@link DataStreamTimestampFieldMapper} and {@link TimeSeriesIdFieldMapper}) can read
     * per-document timestamp values via {@link #timestamps()} without re-scanning
     * the Lucene column list. Mirrors the row-path side channel
     * ({@code DataStreamTimestampFieldMapper.storeTimestampValueForReuse}).
     *
     * @throws IllegalArgumentException if called more than once
     */
    public void setTimestamps(EscfLongColumn timestamps) {
        if (this.timestamps != null) {
            throw new IllegalArgumentException(
                "data stream timestamp field [" + DataStreamTimestampFieldMapper.DEFAULT_PATH + "] encountered multiple values"
            );
        }
        this.timestamps = timestamps;
    }

    /**
     * Returns the {@code @timestamp} column for direct access.
     *
     * @throws IllegalArgumentException if no timestamp column was recorded (mirrors the row path's
     *     "data stream timestamp field [@timestamp] is missing" error from
     *     {@link DataStreamTimestampFieldMapper#extractTimestampValue})
     */
    public EscfLongColumn timestamps() {
        if (timestamps == null) {
            throw new IllegalArgumentException(
                "data stream timestamp field [" + DataStreamTimestampFieldMapper.DEFAULT_PATH + "] is missing"
            );
        }
        return timestamps;
    }

    public Recycler<BytesRef> recycler() {
        return recycler;
    }

    /**
     * Attaches a fully-assembled {@link LuceneColumn} covering all {@code docCount} rows.
     *
     * <p>Use this overload only when the column's backing data is owned by something other than this
     * context — for example, a zero-copy alias into the source batch, or data already registered via
     * {@link #addResource}. When the column owns freshly-allocated recycler buffers, use
     * {@link #addColumn(LuceneColumn, EscfColumnData)} instead so the buffers are released when this
     * context closes.
     */
    public void addColumn(LuceneColumn column) {
        assert frozen == false;
        columns.add(column);
    }

    /**
     * Attaches a {@link LuceneColumn} and registers {@code owned} as a managed resource to be
     * released when this context is {@link #close() closed}.
     */
    public void addColumn(LuceneColumn column, EscfColumnData owned) {
        assert frozen == false;
        columns.add(column);
        resources.add(owned);
    }

    /**
     * Registers a {@link Releasable} resource to be released when this context is
     * {@link #close() closed}, without attaching a Lucene column.
     */
    public void addResource(Releasable resource) {
        assert frozen == false;
        resources.add(resource);
    }

    /**
     * Registers a field's direct {@code columnar_stored} {@code _source} writer. Called from {@link FieldMapper#mapColumnBatch} for a
     * source-contributing field whose mapper supports the direct path for this batch. Ignored once the batch is already marked
     * {@link #markDirectSourceUnavailable() unavailable}, since the whole batch will then take the loader path.
     */
    public void registerSourceColumn(CompositeSourceColumn sourceColumn) {
        assert frozen == false;
        if (directSourceUnavailable == false) {
            sourceColumns.add(sourceColumn);
        }
    }

    /**
     * Enables direct {@code _source} writer construction for the field about to be mapped. Called by {@link FieldMapper#mapColumnBatch}
     * immediately before invoking {@link FieldMapper#doMapColumnBatch} for a source-contributing field whose mapper supports the direct
     * path, and cleared via {@link #clearSourceColumnRequest()} right after. While set, a mapper reads it through
     * {@link #shouldBuildSourceColumn()} and builds its {@link CompositeSourceColumn} in the same pass it maps its Lucene columns.
     */
    void requestSourceColumn() {
        assert frozen == false;
        buildSourceColumnRequested = true;
    }

    /** Clears the per-field request set by {@link #requestSourceColumn()} once the field's {@code doMapColumnBatch} returns. */
    void clearSourceColumnRequest() {
        buildSourceColumnRequested = false;
    }

    /**
     * Whether the field currently being mapped should build its direct {@code columnar_stored} {@code _source} writer during this
     * {@code doMapColumnBatch} pass and register it via {@link #registerSourceColumn}. A mapper that builds nothing while this holds
     * sends the whole batch to the loader path (default-deny, enforced by {@link FieldMapper#mapColumnBatch}).
     */
    public boolean shouldBuildSourceColumn() {
        return buildSourceColumnRequested;
    }

    /**
     * Marks the batch as unable to use the direct {@code columnar_stored} {@code _source} path, so {@link SourceFieldMapper} rebuilds
     * every row through the synthetic-source loader instead. Irreversible for the batch, and discards any writers registered so far.
     */
    public void markDirectSourceUnavailable() {
        assert frozen == false;
        directSourceUnavailable = true;
        sourceColumns.clear();
    }

    /**
     * Whether every source-contributing field of the batch registered a direct writer, so {@link SourceFieldMapper#postColumnarParse}
     * may build the blob from {@link #sourceColumns()} instead of the loader.
     */
    public boolean directSourceAvailable() {
        return directSourceUnavailable == false;
    }

    /** The registered direct {@code _source} writers; meaningful only when {@link #directSourceAvailable()} holds. */
    public List<CompositeSourceColumn> sourceColumns() {
        return sourceColumns;
    }

    /**
     * Returns a cursor that reassembles, one document at a time, the Lucene fields of every column attached so far — the row-oriented
     * view of the batch, for mappers that must read back what the other mappers produced (e.g. {@code columnar_stored} rebuilding
     * {@code _source}). Unlike {@link #columns()} this does not {@link #frozen freeze} the context, so the caller may still
     * attach or {@link #removeColumnsIf remove} columns afterwards; columns attached later are not seen by the returned cursor.
     */
    public MappedColumns.RowCursor rowCursor() {
        return mappedColumns().rowCursor();
    }

    /**
     * Detaches every column whose Lucene field name matches {@code nameFilter}. The backing data of a detached column stays registered
     * with this context and is released on {@link #close()}.
     */
    public void removeColumnsIf(Predicate<String> nameFilter) {
        assert frozen == false;
        columns.removeIf(column -> nameFilter.test(column.toLuceneColumn().name()));
    }

    @Override
    public void close() {
        Releasables.close(resources);
        resources.clear();
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
     * Returns the size in bytes of document {@code doc}'s source, for accounting rather than storage.
     *
     * <p>A document that arrives as a row of a pre-built {@link SourceBatch} carries no source bytes on its request, so
     * {@link #sources()} cannot size it; its size is estimated from the batch row instead, which is what the row-major path does
     * for row-backed sources (see {@code DocumentSource#estimatedSizeInBytes}). A document with neither has size {@code 0}.
     */
    public int sourceSizeInBytes(int doc) {
        final BytesReference source = batch.sources()[doc];
        if (source != null && source.length() > 0) {
            return source.length();
        }
        final SourceBatch sourceBatch = batch.sourceBatch();
        return sourceBatch != null ? sourceBatch.row(doc).sizeInBytes() : 0;
    }

    /**
     * Returns the {@code _id} (Uid-encoded) array.
     */
    public BytesRef[] uids() {
        return batch.uids();
    }

    /**
     * Returns the plain-text id for document {@code doc}, or {@code null} if not yet assigned.
     * For time-series indices the id is derived during mapping and set via {@link #setSyntheticId}.
     */
    public String id(int doc) {
        return batch.id(doc);
    }

    /**
     * Sets the synthetic {@code _id} and uid for document {@code doc}. Called by the time-series
     * columnar {@code _id} mapper during {@code postColumnarParse}.
     */
    public void setSyntheticId(int doc, String id, BytesRef uid) {
        assert frozen == false;
        batch.setSyntheticId(doc, id, uid);
    }

    /**
     * Returns the coordinator-computed tsid array, or {@code null} if no document in the batch
     * carries a tsid (the common case for non-time-series indices).
     */
    public BytesRef[] tsids() {
        return batch.tsids();
    }

    /**
     * Whether {@code _data_stream_timestamp} is present and enabled for this index..
     */
    public boolean isDataStreamTimestampFieldEnabled() {
        return mappingLookup.isDataStreamTimestampFieldEnabled();
    }

    /**
     * Returns the {@link MappingLookup} for this index. Used by metadata mappers that need to
     * inspect the mapping during {@code postColumnarParse} (e.g. timestamp resolution detection).
     */
    public MappingLookup mappingLookup() {
        return mappingLookup;
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

    /**
     * Whether {@code _source} is stored as a {@code columnar_stored} whole-document blob, the only mode that uses the direct source
     * path driven by {@link #registerSourceColumn} and {@link #sourceColumns()}.
     */
    public boolean isSourceColumnarStored() {
        return mappingLookup.isSourceColumnarStored();
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
        return mappedColumns();
    }

    private MappedColumns mappedColumns() {
        return new MappedColumns(0, batch.docCount(), batch.seqNoBytes(), batch.primaryTermBytes(), batch.versionBytes(), columns);
    }
}
