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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.escf.EscfLongColumn;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.sourcebatch.LuceneColumn;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.MappedColumns.NestedColumns;
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
 *
 * <p>The documents of a nested field are the children of the documents of the batch. They are mapped in a child scope
 * ({@link #forNestedChildren}), a context of its own whose documents are the nested elements of one nested path: it has its own
 * columns and {@code _field_names}, and its own sequence numbers and versions, which the engine fills from those of the parent. It
 * shares the resources of the root, so closing the root context releases them all, and records ignored fields on the root document
 * that owns the element. What only a root document has, like the source, the routing and the timestamp, is not available in a child
 * scope.
 */
public final class BatchMappingContext implements Releasable {

    private final IndexOperationBatch batch;
    /** The context of the batch for a child scope, {@code null} for the context of the batch itself. */
    private final BatchMappingContext root;
    private final NestedObjectMapper nestedMapper;
    private final int docCount;
    private final BytesRef seqNos;
    private final BytesRef primaryTerms;
    private final BytesRef versions;
    private final BytesRef[] uids;
    /** For a child scope: the children of parent {@code d} are the documents {@code [parentOffsets[d], parentOffsets[d + 1])}. */
    private final int[] parentOffsets;
    /** For a child scope: the document of the batch that owns each child. */
    private final int[] childToRow;
    /** The child scopes of the context of the batch, in the order that their children go in a block. */
    private final List<BatchMappingContext> children = new ArrayList<>();
    private final MappingLookup mappingLookup;
    private final IndexSettings indexSettings;
    private final Recycler<BytesRef> recycler;
    private final List<LuceneColumn> columns = new ArrayList<>();
    private final List<Releasable> resources = new ArrayList<>();
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
        this.root = null;
        this.nestedMapper = null;
        this.docCount = batch.docCount();
        this.seqNos = batch.seqNoBytes();
        this.primaryTerms = batch.primaryTermBytes();
        this.versions = batch.versionBytes();
        this.uids = batch.uids();
        this.parentOffsets = null;
        this.childToRow = null;
        this.mappingLookup = mappingLookup;
        this.indexSettings = indexSettings;
        this.recycler = recycler;
        this.fieldNamesFieldMapper = mappingLookup.getMapping().fieldNamesFieldMapper();
    }

    private BatchMappingContext(BatchMappingContext root, NestedObjectMapper nestedMapper, int[] parentOffsets, int[] childToRow) {
        this.batch = root.batch;
        this.root = root;
        this.nestedMapper = nestedMapper;
        this.docCount = childToRow.length;
        this.parentOffsets = parentOffsets;
        this.childToRow = childToRow;
        // The children take the sequence number and version of their parent from the engine, see MappedColumns#setSeqNo. They have no
        // primary term of their own: the row path does not add one to a nested document either.
        this.seqNos = new BytesRef(new byte[docCount * Long.BYTES]);
        this.primaryTerms = new BytesRef();
        this.versions = new BytesRef(new byte[docCount * Long.BYTES]);
        final BytesRef[] parentUids = root.uids;
        this.uids = new BytesRef[docCount];
        for (int c = 0; c < docCount; c++) {
            uids[c] = parentUids[childToRow[c]];
        }
        this.mappingLookup = root.mappingLookup;
        this.indexSettings = root.indexSettings;
        this.recycler = root.recycler;
        this.fieldNamesFieldMapper = root.fieldNamesFieldMapper;
    }

    /**
     * Creates the child scope of {@code nestedMapper}: a context whose documents are the nested elements of that path, in the order
     * of their parents and in source order for each parent. Must be called on the context of the batch.
     *
     * @param parentOffsets the children of parent {@code d} are the elements {@code [parentOffsets[d], parentOffsets[d + 1])}
     * @param childToRow the document of the batch that owns each element
     */
    public BatchMappingContext forNestedChildren(NestedObjectMapper nestedMapper, int[] parentOffsets, int[] childToRow) {
        assert root == null : "a nested field cannot have nested fields in a strict columnar index";
        assert frozen == false;
        assert parentOffsets.length == docCount() + 1 && parentOffsets[docCount()] == childToRow.length;
        final BatchMappingContext child = new BatchMappingContext(this, nestedMapper, parentOffsets, childToRow);
        children.add(child);
        return child;
    }

    /** The nested mapper whose elements are the documents of this child scope, or {@code null} for the context of the batch. */
    @Nullable
    public NestedObjectMapper nestedMapper() {
        return nestedMapper;
    }

    /** Whether this is the context of the batch, as opposed to the child scope of a nested field. */
    public boolean isRoot() {
        return root == null;
    }

    /** Whether any child scope has been created, that is whether documents have nested children. */
    public boolean hasNestedChildren() {
        return children.isEmpty() == false;
    }

    private void assertRoot(String what) {
        if (root != null) {
            throw new IllegalStateException(what + " is only available for the documents of the batch, not for nested children");
        }
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
        assertRoot("the timestamp");
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
        assertRoot("the timestamp");
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
        addResource(owned);
    }

    /**
     * Registers a {@link Releasable} resource to be released when this context is
     * {@link #close() closed}, without attaching a Lucene column.
     */
    public void addResource(Releasable resource) {
        assert frozen == false;
        if (root != null) {
            root.addResource(resource);
        } else {
            resources.add(resource);
        }
    }

    /**
     * Returns a cursor that reassembles, one document at a time, the Lucene fields of every column attached so far — the row-oriented
     * view of the batch, for mappers that must read back what the other mappers produced (e.g. {@code columnar_stored} rebuilding
     * {@code _source}). Unlike {@link #columns()} this does not {@link #frozen freeze} the context, so the caller may still
     * attach or {@link #removeColumnsIf remove} columns afterwards; columns attached later are not seen by the returned cursor.
     *
     * @param include selects the columns that the cursor reads. The fields of the columns that it rejects are left out of the
     *                assembled rows, and these columns are not read at all, which saves the cost to build their fields.
     */
    public MappedColumns.RowCursor rowCursor(Predicate<? super LuceneColumn> include) {
        return mappedColumns().rowCursor(include);
    }

    /**
     * Like {@link #rowCursor}, and the cursor also yields the nested children of each document, before the document itself. Use it
     * when the documents have children, or {@link #hasNestedChildren()} is {@code false}, and the rows are enough.
     */
    public MappedColumns.BlockCursor blockCursor(Predicate<? super LuceneColumn> include) {
        return mappedColumns().blockCursor(include);
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
        return seqNos;
    }

    /**
     * Returns the mutable {@code _primary_term} buffer. Delegated to the underlying
     * {@link IndexOperationBatch#primaryTermBytes()}.
     */
    public BytesRef primaryTerms() {
        assertRoot("the primary term");
        return primaryTerms;
    }

    /**
     * Returns the mutable {@code _version} buffer. Delegated to the underlying
     * {@link IndexOperationBatch#versionBytes()}.
     */
    public BytesRef versions() {
        return versions;
    }

    /**
     * Returns the routing array, or {@code null} if no document in the chunk has an explicit
     * routing (the common case). When non-null, individual entries may still be {@code null} for
     * documents without routing.
     */
    public BytesRef[] routings() {
        assertRoot("the routing");
        return batch.routings();
    }

    /** Returns the per-document content-type array; entries default to {@link XContentType#JSON} when the request had none. */
    public XContentType[] contentTypes() {
        assertRoot("the content type");
        return batch.contentTypes();
    }

    /**
     * Returns the per-document source array. Individual entries may be {@code null} for documents
     * that carry no source.
     */
    public BytesReference[] sources() {
        assertRoot("the source");
        return batch.sources();
    }

    /**
     * Returns the size in bytes of every document's source, indexed by document, for accounting rather than storage.
     *
     * <p>A document that arrives as a row of a pre-built {@link SourceBatch} carries no source bytes on its request, so
     * {@link #sources()} cannot size it; its size is estimated from the batch row instead, which is what the row-major path does
     * for row-backed sources (see {@code DocumentSource#estimatedSizeInBytes}). The batch sizes all of its rows in one column
     * sweep ({@link SourceBatch#rowSizesInBytes()}), which is done once and only when at least one document needs it. A document
     * with neither has size {@code 0}.
     */
    public int[] sourceSizesInBytes() {
        assertRoot("the source size");
        final int docCount = batch.docCount();
        final BytesReference[] sources = batch.sources();
        final int[] sizes = new int[docCount];
        int[] batchSizes = null;
        boolean batchSized = false;
        for (int d = 0; d < docCount; d++) {
            final BytesReference source = sources[d];
            if (source != null && source.length() > 0) {
                sizes[d] = source.length();
                continue;
            }
            if (batchSized == false) {
                final SourceBatch sourceBatch = batch.sourceBatch();
                batchSizes = sourceBatch != null ? sourceBatch.rowSizesInBytes() : null;
                batchSized = true;
            }
            if (batchSizes != null) {
                sizes[d] = batchSizes[d];
            }
        }
        return sizes;
    }

    /**
     * Returns the {@code _id} (Uid-encoded) array.
     */
    public BytesRef[] uids() {
        return uids;
    }

    /**
     * Returns the plain-text id for document {@code doc}, or {@code null} if not yet assigned.
     * For time-series indices the id is derived during mapping and set via {@link #setSyntheticId}.
     */
    public String id(int doc) {
        return batch.id(root != null ? childToRow[doc] : doc);
    }

    /**
     * Returns the normalized size metered for document {@code doc} on the primary, or
     * {@link org.elasticsearch.plugins.internal.XContentMeteringParserDecorator#UNKNOWN_SIZE} if the batch was not
     * metered. Lets a metadata mapper persist the metered size as a column: the columnar counterpart of adding a field to
     * {@link ParsedDocument#rootDoc()} from {@code DocumentSizeReporter#onParsingCompleted}, which has no
     * {@link LuceneDocument} to add to on this path.
     */
    public long normalizedSize(int doc) {
        assertRoot("the metered size");
        return batch.normalizedSize(doc);
    }

    /**
     * Sets the synthetic {@code _id} and uid for document {@code doc}. Called by the time-series
     * columnar {@code _id} mapper during {@code postColumnarParse}.
     */
    public void setSyntheticId(int doc, String id, BytesRef uid) {
        assertRoot("the synthetic id");
        assert frozen == false;
        batch.setSyntheticId(doc, id, uid);
    }

    /**
     * Returns the coordinator-computed tsid array, or {@code null} if no document in the batch
     * carries a tsid (the common case for non-time-series indices).
     */
    public BytesRef[] tsids() {
        assertRoot("the time series id");
        return batch.tsids();
    }

    /**
     * Whether {@code _data_stream_timestamp} is present and enabled for this index..
     */
    public boolean isDataStreamTimestampFieldEnabled() {
        return root == null && mappingLookup.isDataStreamTimestampFieldEnabled();
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
            fieldNames = new DeduplicatingStringColumnAccumulator(docCount);
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
        if (root != null) {
            // Like the row path: the _ignored field of a nested element is recorded on the root document that owns it.
            root.addIgnoredFieldColumnar(childToRow[doc], field);
            return;
        }
        if (ignoredFields == null) {
            ignoredFields = new DeduplicatingStringColumnAccumulator(docCount);
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
        return docCount;
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
        for (BatchMappingContext child : children) {
            child.frozen = true;
        }
        return mappedColumns();
    }

    private MappedColumns mappedColumns() {
        if (children.isEmpty()) {
            return new MappedColumns(0, docCount, seqNos, primaryTerms, versions, columns);
        }
        final List<NestedColumns> nested = new ArrayList<>(children.size());
        for (BatchMappingContext child : children) {
            nested.add(new NestedColumns(child.nestedMapper.nestedTypePath(), child.ownColumns(), child.parentOffsets));
        }
        return new MappedColumns(0, docCount, seqNos, primaryTerms, versions, columns, nested);
    }

    /** The columns of a child scope, which are the rows of one nested path. */
    private MappedColumns ownColumns() {
        return new MappedColumns(0, docCount, seqNos, primaryTerms, versions, columns);
    }
}
