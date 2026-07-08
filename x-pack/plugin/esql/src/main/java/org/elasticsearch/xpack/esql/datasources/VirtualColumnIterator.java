/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Producer-side wrapper that augments pages from the format reader with constant blocks for virtual
 * partition / {@code _file.*} columns. The reader emits data-only pages; this iterator slots the
 * data blocks into the unified output schema and fills the remaining slots with constant blocks
 * carrying the partition values for the file currently being read.
 * <p>
 * Memory: constant blocks are allocated against the same {@link BlockFactory} the reader uses
 * (the root, request-scoped factory), so they are charged to the global request circuit breaker
 * just like the data blocks they accompany. This avoids the driver-local breaker's single-thread
 * assertion firing when the producer drains pages off a generic-pool thread.
 *
 * @see AsyncExternalSourceOperatorFactory for where this iterator is attached.
 */
final class VirtualColumnIterator implements CloseableIterator<Page> {

    private final CloseableIterator<Page> delegate;
    private final List<Attribute> fullOutput;
    private final Map<String, Object> partitionValues;
    private final BlockFactory blockFactory;
    private final int[] dataColumnIndices;
    private final int[] partitionColumnIndices;
    /**
     * Per-file prefix bytes for {@code _id} composition, or {@code null} when {@code _id} is not
     * projected. When non-null, composes {@code <prefix><rowPosition>} per row into the {@code _id}
     * output slot from the reader-emitted {@link ColumnExtractor#ROW_POSITION_COLUMN} block.
     */
    @Nullable
    private final BytesRef idPrefix;
    /**
     * Index of {@link ColumnExtractor#ROW_POSITION_COLUMN} within {@link #dataColumnIndices} (i.e.
     * position in the incoming data page's block list), or {@code -1} when not present. Non-negative
     * whenever {@code _id} or {@code _file.record_ref} is requested, since both are composed from it.
     */
    private final int rowPositionDataChannel;
    /** Index of {@code _id} within {@link #fullOutput}, or {@code -1} when not present. */
    private final int idOutputIndex;
    /**
     * Index of {@code _file.record_ref} within {@link #fullOutput}, or {@code -1} when not present.
     * When non-negative the iterator fills that slot with the masked physical position from the
     * reader-emitted {@link ColumnExtractor#ROW_POSITION_COLUMN} channel — the same value {@code _id}
     * is composed from, exposed directly as the opaque per-record token.
     */
    private final int recordRefOutputIndex;
    /**
     * Index of {@code _source} within {@link #fullOutput}, or {@code -1} when not present. When
     * non-negative the iterator builds the per-row JSON for that slot via
     * {@link SynthesizeExternalSource#composePage}.
     */
    private final int sourceOutputIndex;
    /**
     * Names of the data columns paired positionally with the incoming data page's blocks. Cached
     * at construction so the per-page {@code _source} synthesis does not re-walk
     * {@link #fullOutput} on every row.
     */
    private final String[] dataColumnNames;
    /**
     * Declared {@link DataType}s of the data columns, paired positionally with
     * {@link #dataColumnNames}. {@code _source} synthesis needs the type to render each value the
     * way the response layer would (IP/VERSION/DATETIME as strings, UNSIGNED_LONG decoded) rather
     * than as raw block contents.
     */
    private final DataType[] dataColumnTypes;
    /**
     * Index of the declared {@code _id.path} column within {@link #dataColumnIndices} (i.e. its position in the incoming
     * data page's block list), or {@code -1} when the dataset declares no {@code _id.path} (synthetic {@code _id}) or the
     * id column is not present in this file's projection (a UBN-missing carve-out → null {@code _id}). When non-negative
     * and {@code _id} is projected, each row's {@code _id} is rendered from that column's block instead of the synthetic
     * (file+row-position) identity. The column travels the same data-page channel as any other projected data column, so
     * it stays row-aligned with the rest of the page positionally.
     */
    private final int idPathDataChannel;
    /**
     * Declared {@link DataType} of the {@code _id.path} column, or {@code null} when {@link #idPathDataChannel} is
     * {@code -1}. Needed to render the column's block to KEYWORD the way {@code TO_STRING} would (DATETIME/DATE_NANOS as
     * ISO strings, numerics/booleans stringified, KEYWORD/TEXT passed through).
     */
    @Nullable
    private final DataType idPathDataType;
    /**
     * The declared {@code _id.path} logical column name, or {@code null} when the dataset declares no {@code _id.path}
     * (synthetic {@code _id}). Retained (beyond {@link #idPathDataChannel}) so the iterator distinguishes "id column
     * declared but absent from this file's projection" (UBN carve-out → null {@code _id}) from "no id path declared"
     * (synthetic path). Only consulted when {@code _id} is projected.
     */
    @Nullable
    private final String idPath;

    VirtualColumnIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> fullOutput,
        Set<String> partitionColumnNames,
        Map<String, Object> partitionValues,
        BlockFactory blockFactory
    ) {
        this(delegate, fullOutput, partitionColumnNames, partitionValues, blockFactory, null, null);
    }

    VirtualColumnIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> fullOutput,
        Set<String> partitionColumnNames,
        Map<String, Object> partitionValues,
        BlockFactory blockFactory,
        @Nullable BytesRef idPrefix
    ) {
        this(delegate, fullOutput, partitionColumnNames, partitionValues, blockFactory, idPrefix, null);
    }

    /**
     * Variant that wires {@code _id} composition. {@code idPrefix} must be non-null whenever the
     * {@code _id} name is in {@code partitionColumnNames}; the source factory builds it once per
     * file via {@link ExternalRowIdentity#prefix(org.elasticsearch.xpack.esql.datasources.spi.StoragePath, long)}
     * and reuses it across every page of that file. When {@code _id} is not requested,
     * {@code idPrefix} should be {@code null} and the iterator behaves identically to the
     * legacy two-arg constructor.
     * <p>
     * {@code idPath} is the logical name of a data column declared via {@code mappings._id.path}. When non-null and
     * {@code _id} is projected, each row's {@code _id} is rendered from that column's value (KEYWORD, null→null,
     * multi-valued→reject) instead of the synthetic {@code idPrefix}-based identity. When the id column is not part of
     * this file's projection (UBN carve-out), {@code _id} renders as SQL {@code NULL}. {@code null} restores the
     * synthetic path byte-for-byte.
     */
    VirtualColumnIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> fullOutput,
        Set<String> partitionColumnNames,
        Map<String, Object> partitionValues,
        BlockFactory blockFactory,
        @Nullable BytesRef idPrefix,
        @Nullable String idPath
    ) {
        Check.notNull(delegate, "delegate cannot be null");
        Check.isTrue(fullOutput != null && fullOutput.isEmpty() == false, "fullOutput cannot be null or empty");
        Check.notNull(partitionColumnNames, "partitionColumnNames cannot be null");
        Check.notNull(blockFactory, "blockFactory cannot be null");
        this.delegate = delegate;
        this.fullOutput = fullOutput;
        this.partitionValues = partitionValues != null ? partitionValues : Map.of();
        this.blockFactory = blockFactory;
        this.idPrefix = idPrefix;

        List<Integer> dataIdxList = new ArrayList<>();
        List<Integer> partIdxList = new ArrayList<>();
        List<String> dataNames = new ArrayList<>();
        List<DataType> dataTypes = new ArrayList<>();
        int idIdx = -1;
        int sourceIdx = -1;
        int recordRefIdx = -1;
        int rowPosChannelInData = -1;
        int idPathChannelInData = -1;
        DataType idPathType = null;
        int nextDataChannel = 0;
        for (int i = 0; i < fullOutput.size(); i++) {
            String name = fullOutput.get(i).name();
            if (partitionColumnNames.contains(name)) {
                partIdxList.add(i);
                if (ExternalMetadataColumns.ID.equals(name)) {
                    idIdx = i;
                } else if (ExternalMetadataColumns.SOURCE.equals(name)) {
                    sourceIdx = i;
                } else if (FileMetadataColumns.RECORD_REF.equals(name)) {
                    recordRefIdx = i;
                }
            } else {
                dataIdxList.add(i);
                dataNames.add(name);
                DataType type = fullOutput.get(i).dataType();
                dataTypes.add(type);
                if (ColumnExtractor.ROW_POSITION_COLUMN.equals(name)) {
                    rowPosChannelInData = nextDataChannel;
                }
                // The _id.path column is a normal projected data column: PruneColumns pins it into the reader
                // projection (so it rides the same data page even when the query did not KEEP it) and it travels
                // this same data-page channel as every other data block, keeping it row-aligned with the page.
                if (idPath != null && idPath.equals(name)) {
                    idPathChannelInData = nextDataChannel;
                    idPathType = type;
                }
                nextDataChannel++;
            }
        }
        this.dataColumnIndices = toIntArray(dataIdxList);
        this.partitionColumnIndices = toIntArray(partIdxList);
        this.idOutputIndex = idIdx;
        this.recordRefOutputIndex = recordRefIdx;
        this.sourceOutputIndex = sourceIdx;
        this.rowPositionDataChannel = rowPosChannelInData;
        this.idPathDataChannel = idPathChannelInData;
        this.idPathDataType = idPathType;
        this.idPath = idPath;
        this.dataColumnNames = dataNames.toArray(new String[0]);
        this.dataColumnTypes = dataTypes.toArray(new DataType[0]);
        Check.isTrue(idPrefix == null || idIdx >= 0, "idPrefix supplied but _id slot missing from fullOutput");
        // Synthetic _id and _file.record_ref are both composed from the reader-emitted _rowPosition channel,
        // which the optimizer injects whenever either is requested. If the slot is present but the
        // channel is missing, fail loud rather than silently emit wrong identities. A declared _id.path uses the
        // named data column instead of the synthetic path, so it does not require the _rowPosition channel.
        Check.isTrue(
            idIdx < 0 || idPath != null || rowPosChannelInData >= 0,
            "_id requested but reader did not emit the _rowPosition channel"
        );
        Check.isTrue(
            recordRefIdx < 0 || rowPosChannelInData >= 0,
            "_file.record_ref requested but reader did not emit the _rowPosition channel"
        );
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Page next() {
        return inject(delegate.next());
    }

    @Override
    public SubscribableListener<Void> waitForReady() {
        return delegate.waitForReady();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Public for unit tests.
     * <p>
     * On success the input page's data blocks are reused inside the returned page; any extra
     * blocks the producer emitted beyond the configured projection are released here so the
     * iterator never silently drops a refcount. On failure both the partial constant-block
     * allocations and the input page's blocks are released so the caller does not have to
     * clean up the input page separately.
     */
    Page inject(Page dataPage) {
        if (partitionColumnIndices.length == 0) {
            // Any extra blocks the producer emitted beyond what we project would otherwise leak
            // when the dataPage reference is dropped without releaseBlocks(). Forward as-is when
            // the block count matches; otherwise rebuild a tightly-projected page and release the
            // surplus blocks.
            int producedBlocks = dataPage.getBlockCount();
            if (producedBlocks == dataColumnIndices.length) {
                return dataPage;
            }
            return projectAndReleaseSurplus(dataPage);
        }

        int positions = dataPage.getPositionCount();
        Block[] blocks = new Block[fullOutput.size()];

        int producedBlocks = dataPage.getBlockCount();
        int expectedDataBlocks = dataColumnIndices.length;
        // Same under-projection guard as projectAndReleaseSurplus: fail loud before touching block
        // refcounts so a contract-breaking reader cannot leak the page through an out-of-bounds read.
        if (producedBlocks < expectedDataBlocks) {
            dataPage.releaseBlocks();
            throw new IllegalStateException(
                "format reader produced " + producedBlocks + " blocks, projection expects " + expectedDataBlocks
            );
        }
        int dataBlockIdx = 0;
        for (int idx : dataColumnIndices) {
            blocks[idx] = dataPage.getBlock(dataBlockIdx++);
        }

        int partitionBlocksAllocated = 0;
        try {
            for (int idx : partitionColumnIndices) {
                Attribute attr = fullOutput.get(idx);
                if (idx == idOutputIndex && idPath != null) {
                    // Declared _id.path: stamp _id from the named data column's value rather than the synthetic
                    // identity. The column rides the data page at idPathDataChannel (PruneColumns pinned it into the
                    // reader projection), so it is row-aligned with the page positionally. When the column is absent
                    // from this file's projection (UBN carve-out), _id renders as SQL NULL.
                    if (idPathDataChannel >= 0) {
                        Block idColumnBlock = dataPage.getBlock(idPathDataChannel);
                        blocks[idx] = composeIdFromColumn(idColumnBlock, idPathDataType, positions);
                    } else {
                        blocks[idx] = blockFactory.newConstantNullBlock(positions);
                    }
                } else if (idx == idOutputIndex && idPrefix != null) {
                    // _id = base64url(hash(location) | mtime | masked physical position) from the reader-emitted
                    // _rowPosition channel. Every file reader emits this channel (the optimizer
                    // injects it for _id / _file.record_ref); a split-local counter would reset to 0
                    // each split and break _id repeatability across layouts.
                    Block rowPosBlock = dataPage.getBlock(rowPositionDataChannel);
                    blocks[idx] = ExternalRowIdentity.composePage(idPrefix, (LongBlock) rowPosBlock, blockFactory);
                } else if (idx == recordRefOutputIndex) {
                    // _file.record_ref = the same masked physical position, surfaced directly as the
                    // opaque per-record LONG token (no location prefix).
                    Block rowPosBlock = dataPage.getBlock(rowPositionDataChannel);
                    blocks[idx] = buildRecordRefBlock((LongBlock) rowPosBlock, positions);
                } else if (idx == sourceOutputIndex) {
                    // Materialize _source from this row's data column values: project the data
                    // blocks already pulled off the page above into a (names, blocks) pair and
                    // hand to the shared serializer.
                    Block[] dataBlocksByOrder = new Block[dataColumnIndices.length];
                    for (int d = 0; d < dataColumnIndices.length; d++) {
                        dataBlocksByOrder[d] = blocks[dataColumnIndices[d]];
                    }
                    blocks[idx] = SynthesizeExternalSource.composePage(
                        dataColumnNames,
                        dataColumnTypes,
                        dataBlocksByOrder,
                        positions,
                        blockFactory
                    );
                } else {
                    Object value = partitionValues.get(attr.name());
                    blocks[idx] = createConstantBlock(attr, value, positions);
                }
                partitionBlocksAllocated++;
            }
            Page result = new Page(positions, blocks);
            // Producer over-projected (e.g. format reader fell back to the full file schema when the
            // projection list was empty). Release the surplus only on the success path: in the catch
            // arm below, {@link Page#releaseBlocks} on dataPage closes every block in the page —
            // including these — so an early surplus close here would double-close on failure.
            if (producedBlocks > expectedDataBlocks) {
                for (int i = expectedDataBlocks; i < producedBlocks; i++) {
                    Block extra = dataPage.getBlock(i);
                    if (extra != null) {
                        extra.close();
                    }
                }
            }
            return result;
        } catch (Throwable t) {
            for (int i = 0, released = 0; i < partitionColumnIndices.length && released < partitionBlocksAllocated; i++) {
                Block b = blocks[partitionColumnIndices[i]];
                if (b != null) {
                    b.close();
                    released++;
                }
            }
            dataPage.releaseBlocks();
            throw t;
        }
    }

    /**
     * Builds a page containing only the projected data blocks and releases the surplus. Used when
     * there are no partition columns to inject but the producer over-projected.
     * <p>
     * Producer invariant: every format reader emits data columns at the head of the page in the
     * order declared by {@link #dataColumnNames}; surplus blocks (e.g. the parquet-mr "empty
     * projection → full schema" fallback) trail at higher indices. This method drops the trailing
     * surplus; callers that emit a permuted block order will silently lose data.
     */
    private Page projectAndReleaseSurplus(Page dataPage) {
        int positions = dataPage.getPositionCount();
        int expected = dataColumnIndices.length;
        // Under-projection means the reader broke its contract — fail loud before touching block
        // refcounts, releasing the whole page so nothing leaks (mirrors the inject() cleanup).
        if (dataPage.getBlockCount() < expected) {
            int produced = dataPage.getBlockCount();
            dataPage.releaseBlocks();
            throw new IllegalStateException("format reader produced " + produced + " blocks, projection expects " + expected);
        }
        try {
            Block[] kept = new Block[expected];
            for (int i = 0; i < expected; i++) {
                kept[i] = dataPage.getBlock(i);
            }
            for (int i = expected; i < dataPage.getBlockCount(); i++) {
                Block extra = dataPage.getBlock(i);
                if (extra != null) {
                    extra.close();
                }
            }
            return new Page(positions, kept);
        } catch (Throwable t) {
            dataPage.releaseBlocks();
            throw t;
        }
    }

    /**
     * Composes the {@code _id} KEYWORD block from the declared {@code _id.path} column's block. Rules: a {@code null}
     * cell renders a {@code null} {@code _id}; a multi-valued cell is rejected ({@code _id} must be scalar); a scalar
     * cell is rendered to its KEYWORD form the way {@code TO_STRING(col)} / the response layer would (DATETIME /
     * DATE_NANOS as ISO strings, IP / VERSION decoded, numerics / booleans stringified, KEYWORD / TEXT passed through),
     * so the stamped {@code _id} reads identically to the column's own value. The rendering mirrors
     * {@link ExternalScalarRenderer#render}, keeping {@code _id} and {@code _source} value rendering consistent.
     */
    private Block composeIdFromColumn(Block idColumnBlock, DataType idColumnType, int positions) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = idColumnBlock.getValueCount(pos);
                if (idColumnBlock.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count > 1) {
                    throw new IllegalArgumentException(
                        "_id.path column [" + idPath + "] produced a multi-valued value at a row; _id must be a single scalar value per row"
                    );
                } else {
                    Object value = BlockUtils.toJavaObject(idColumnBlock, pos);
                    builder.appendBytesRef(new BytesRef(renderIdScalar(value, idColumnType)));
                }
            }
            return builder.build();
        }
    }

    /**
     * Renders one scalar {@code _id.path} value to its KEYWORD string form: the shared
     * {@link ExternalScalarRenderer} produces the native value the response layer would (so {@code _id} and
     * {@code _source} stay consistent), stringified here because {@code _id} is KEYWORD.
     */
    private static String renderIdScalar(Object value, DataType type) {
        return String.valueOf(ExternalScalarRenderer.render(value, type));
    }

    boolean hasPartitionColumns() {
        return partitionColumnIndices.length > 0;
    }

    List<String> dataColumnNames() {
        List<String> names = new ArrayList<>(dataColumnIndices.length);
        for (int idx : dataColumnIndices) {
            names.add(fullOutput.get(idx).name());
        }
        return names;
    }

    private static int[] toIntArray(List<Integer> list) {
        int[] result = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            result[i] = list.get(i);
        }
        return result;
    }

    /**
     * Builds the {@code _file.record_ref} block: the masked physical position from the reader-emitted
     * {@code _rowPosition} channel, surfaced as an opaque per-record LONG. The mask strips any
     * deferred-extraction extractor id packed into the high bits (a no-op for unencoded values from
     * the row-index / byte-offset readers). Null positions propagate to null.
     */
    private Block buildRecordRefBlock(LongBlock rowPositionBlock, int positions) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                if (rowPositionBlock.isNull(i)) {
                    builder.appendNull();
                } else {
                    long encoded = rowPositionBlock.getLong(rowPositionBlock.getFirstValueIndex(i));
                    builder.appendLong(encoded & ExternalRowIdentity.LOCAL_POSITION_MASK);
                }
            }
            return builder.build();
        }
    }

    private Block createConstantBlock(Attribute attr, Object value, int positions) {
        return switch (value) {
            case null -> blockFactory.newConstantNullBlock(positions);
            case Integer intVal -> blockFactory.newConstantIntBlockWith(intVal, positions);
            case Long longVal -> blockFactory.newConstantLongBlockWith(longVal, positions);
            case Double doubleVal -> blockFactory.newConstantDoubleBlockWith(doubleVal, positions);
            case Boolean boolVal -> blockFactory.newConstantBooleanBlockWith(boolVal, positions);
            case BytesRef bytesRef -> blockFactory.newConstantBytesRefBlockWith(bytesRef, positions);
            case String stringVal -> blockFactory.newConstantBytesRefBlockWith(new BytesRef(stringVal), positions);
            // No stringify fallback: an unenumerated value type (a future extractor returning
            // Instant, Float, ...) must be rendered intentionally, not as toString() bytes.
            default -> throw new EsqlIllegalArgumentException(
                "cannot render constant column [" + attr.name() + "] from value type [" + value.getClass().getName() + "]"
            );
        };
    }
}
