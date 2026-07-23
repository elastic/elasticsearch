/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

/**
 * Loads one projected column by physical row identity.
 * <p>
 * Deferred extraction keeps the forward scan narrow (sort keys, predicates, and the synthetic
 * routing column) and materializes wide projection-only columns only after a per-driver TopN has
 * chosen surviving rows.
 *
 * <h2>Addressing space (physical row identity)</h2>
 * The {@code positions} accepted by {@link #extract} are <em>row identities</em>: stable, opaque
 * {@code long}s the iterator that emits {@link #ROW_POSITION_COLUMN} attached to each row. The
 * extractor and the iterator must agree on the encoding — for the Parquet implementation, it is
 * the file-global row index in footer iteration order, which is filter-, late-materialization-,
 * and page-skip-invariant. This makes the handshake robust to whatever survives between scan and
 * extract: the same row coming back later carries the same identity that pointed at it during the
 * forward scan, no matter how many predicate / page-skipping transforms ran in between.
 * <p>
 * The handshake between an iterator and its matching extractor is expressed by
 * {@link ColumnExtractorProducer}: an iterator that emits {@link #ROW_POSITION_COLUMN} implements
 * that interface and produces an extractor whose addressing space matches its own.
 * <p>
 * Composition across multiple sources opened in the same driver — assigning extractor ids,
 * encoding references, and dispatching extractions — is handled by {@code SourceExtractors}
 * ({@code org.elasticsearch.xpack.esql.datasources.SourceExtractors}), not by this SPI.
 * <p>
 * Threading: a single driver thread owns an instance; implementations need not be thread-safe.
 */
public interface ColumnExtractor extends Releasable {

    /**
     * Bit position of the extractor id within an encoded {@code _rowPosition} value. The low
     * {@code LOCAL_POSITION_BITS} hold the per-extractor physical row identity; the high bits
     * hold the registry-assigned extractor id. This is the wire-format the iterator implementing
     * {@link ColumnExtractorProducer} must respect when it pre-encodes its emitted values.
     * <p>
     * The framework's {@code SourceExtractors} owns the canonical encoding/decoding helpers and
     * pins this same value; the constant is mirrored here so format readers depend only on the
     * SPI when implementing the encoding handshake.
     */
    int LOCAL_POSITION_BITS = 48;

    /**
     * Reserved name of the synthetic row-reference column emitted by source readers that support
     * deferred materialization. Values are opaque encoded references combining an extractor id with
     * a physical row identity — see {@code org.elasticsearch.xpack.esql.datasources.SourceExtractors}
     * for the bit layout.
     * <p>
     * Encoding lives at the iterator boundary: the source factory wires an extractor id into the
     * iterator via {@link ColumnExtractorProducer#setExtractorId(int)}, and from then on every
     * {@code _rowPosition} value the iterator emits is already encoded as
     * {@code (id << 48) | physicalRowOffset}. Downstream operators decode without a separate
     * encoding pass. The extract operator removes this channel from its output.
     * <p>
     * Historically the encoding ran in a wrapper iterator that rebuilt the {@code _rowPosition}
     * block per page. That wrapper was removed once iterators began materialising the values
     * themselves; if a future format reader cannot pre-encode in-line, re-introducing a similar
     * wrapper is a viable (if slower) fallback — the contract surfaces here so the option remains
     * discoverable.
     */
    String ROW_POSITION_COLUMN = "_rowPosition";

    /**
     * Number of distinct row identities addressable by this extractor.
     * <p>
     * The valid range for {@link #extract}'s {@code positions} entries is {@code [0, rowCount())}.
     * For Parquet this is the file's total row count.
     * <p>
     * Implementations should return a pure in-memory value without triggering I/O.
     *
     * @return number of rows addressable by this extractor
     */
    long rowCount();

    /**
     * Materializes one or more columns at the given row identities in a single I/O batch.
     * <p>
     * Callers invoke this after TopN with only the identities that survived pruning. Identities may
     * be unsorted and may repeat; the returned blocks must align row {@code i} with
     * {@code positions[i]} so operators can zip deferred values with encoded references without an
     * extra reordering pass. Each entry must lie in {@code [0, rowCount())}.
     * <p>
     * Implementations should fetch every requested column's bytes in one coalesced batch so that
     * latency and per-request overhead are amortised across the projection. For object-store
     * backends like S3 the win is significant: the cost dominator is the per-request RTT/TTFB,
     * so issuing one fan-out batch covering all columns instead of N sequential batches turns
     * a {@code O(N)} round-trip wait into {@code O(1)}. For physical layouts where columns within
     * a row group are contiguous (e.g. Parquet column chunks), the bytes for multiple columns also
     * coalesce naturally during merging.
     * <p>
     * Returned blocks are owned by the caller; on partial failure, the implementation must
     * release any blocks it already built before propagating the exception.
     * <p>
     * {@code targetTypes} carries the planner/declared type per column so extraction honors the
     * same declared-type coercion the eager decode paths run ({@code DeclaredTypeCoercions}): a
     * column whose file type differs from its target coerces value-by-value (per-value failures
     * null the cell and emit a response Warning header, bulk-API style). {@code null} — the whole
     * array or an entry — means "emit the file's own type" (no coercion).
     *
     * @param columnNames   logical columns to load, in output order; must not contain
     *                      {@link #ROW_POSITION_COLUMN}
     * @param targetTypes   planner/declared type per column, aligned with {@code columnNames};
     *                      {@code null} (array or entry) disables coercion for that column
     * @param positions     row identities; every element is read
     * @param blockFactory  factory for breaker-aware allocation
     * @return one block per requested column, each with exactly {@code positions.length} values
     *         in caller order
     * @throws IOException if the format reader fails while satisfying the request
     */
    Block[] extract(String[] columnNames, @Nullable DataType[] targetTypes, long[] positions, BlockFactory blockFactory) throws IOException;

    /**
     * Convenience overload for the common single-column case. Delegates to
     * {@link #extract(String[], DataType[], long[], BlockFactory)} so implementations only need to
     * provide one code path.
     */
    default Block extract(String columnName, long[] positions, BlockFactory blockFactory) throws IOException {
        Block[] blocks = extract(new String[] { columnName }, null, positions, blockFactory);
        if (blocks.length != 1) {
            Releasables.closeExpectNoException(blocks);
            throw new IllegalStateException("extract returned " + blocks.length + " blocks for one requested column");
        }
        return blocks[0];
    }
}
