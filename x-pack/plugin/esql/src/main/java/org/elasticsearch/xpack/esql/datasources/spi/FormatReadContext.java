/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;
import java.util.function.Consumer;

/**
 * Immutable context for a single {@link FormatReader#read} or {@link FormatReader#readAsync} call.
 * Bundles all per-read execution parameters that were previously spread across 12+ method overloads.
 * <p>
 * Format-specific configuration (delimiter, encoding, etc.) lives on the reader instance via
 * {@link FormatReader#withConfig}. Per-query optimizer hints (pushed filters) live on the reader
 * instance via {@link FormatReader#withPushedFilter}. This context carries only the parameters
 * that may vary per file or per split within a single query execution.
 *
 * @param projectedColumns columns to read. {@code null} means "no projection info available — read
 *                         every column" (backward compatibility default). An <em>empty</em> list
 *                         means "the optimizer pruned every column" (e.g. {@code COUNT(*)}); format
 *                         readers may take a fast path that skips type conversion and emits row-
 *                         count-only {@link org.elasticsearch.compute.data.Page Page}s.
 * @param batchSize        target number of rows per page
 * @param rowLimit         maximum total rows to return ({@link FormatReader#NO_LIMIT} for unlimited)
 * @param errorPolicy      how to handle malformed rows
 * @param firstSplit       whether this is the first split for the file (consistent with {@code lastSplit};
 *                         format-agnostic replacement for the legacy {@code skipFirstLine} parameter)
 * @param lastSplit        whether this is the last split for the file (affects trailing-record handling)
 * @param recordAligned    whether the split starts at a record boundary (no leading partial record).
 *                         When {@code false}, line-oriented readers may need to skip a leading partial line
 *                         (e.g. bzip2 / zstd-indexed macro-splits). When {@code true}, the split is known
 *                         to start exactly on a record boundary (e.g. streaming-parallel chunks sliced on
 *                         {@code \n}). Has no effect on the first split.
 * @param readSchema       optional planner-resolved positional column layout. When non-{@code null},
 *                         format readers use it as the authoritative typed schema; when {@code null},
 *                         readers fall back to per-file inference. Distinct from
 *                         {@link FormatReader#withSchema}, which carries the projection. Empty
 *                         list and {@code null} both mean "no schema"; the compact constructor
 *                         collapses empty to {@code null} so readers do one check.
 * @param splitStartByte   file-global byte offset at which this split begins (i.e. {@code FileSplit.offset()}).
 *                         Text readers add the bytes they consume to this anchor to emit a file-global,
 *                         split-invariant start byte per record for the {@code _rowPosition} channel
 *                         (the substrate of {@code _file.record_ref} / {@code _id}). {@code 0} for the
 *                         whole-file (non-split) case and for columnar formats, which derive a file-global
 *                         row index from their own footer/stripe metadata rather than from a byte anchor.
 *                         <p>Note: this carries the SAME VALUE as {@code statsBaseOffset} at every current call
 *                         site, but they are distinct CONTRACTS, not one quantity: {@code splitStartByte} may be a
 *                         COMPRESSED coordinate under compressed-offset splits ({@code COMPRESSED_OFFSET_SPLIT_KEY}),
 *                         whereas {@code statsBaseOffset} is decompressed-stream only. Stripe capture is disabled
 *                         exactly where the two would diverge, so they must stay separate — a blind unification
 *                         would mis-address stripes on compressed splits.
 * @param maxRecordBytes   maximum bytes a single text record may occupy while split/trim code
 *                         scans for a record boundary.
 * @param statsBaseOffset  the DECOMPRESSED-stream byte offset of this read's first byte, used by the reader to
 *                         address records to canonical stripes
 *                         ({@code ordinal = floor((statsBaseOffset + recordOffsetInRead) / statsStripeSize)}).
 *                         Ignored when {@code statsStripeSize <= 0}. Same value as {@code splitStartByte} at every
 *                         current call site, but a distinct contract under compressed-offset splits (see its note).
 * @param statsStripeSize  canonical-stripe grid in bytes for per-stripe stats attribution, or
 *                         {@code <= 0} to disable stripe addressing (the reader then emits no
 *                         stripe-addressed contributions and the warm short-circuit safe-misses). A
 *                         pure stats overlay — it never affects how the read is chunked or split.
 * @param statsFileFinal   whether this read reaches the file's true end (the segmentator's EOF chunk,
 *                         or the segmented coordinator's trailing segment). Only the file-final read
 *                         may mark its last stripe complete-on-the-right ({@code atStripeEnd}) and
 *                         terminal ({@code eof}); a non-final chunk ends mid-stripe at a chunk boundary,
 *                         so its trailing stripe is a partial right fragment the next chunk continues.
 *                         Marking a non-final chunk's trailing stripe complete would silently undercount.
 * @param statsColumnScope how much per-stripe statistics the read harvests while it scans (row count
 *                         only / row count + projected columns / row count + all file columns / nothing).
 *                         Orthogonal to {@code statsStripeSize}: the grid decides which stripe a record
 *                         lands in, this decides what is summarised per stripe. {@code null} defaults to
 *                         {@link StripeColumnScope#PROJECTED} (back-compat for call sites that predate the
 *                         setting); the compact constructor collapses {@code null} to that default so
 *                         readers do one check.
 * @param informationalWarningSink optional relay for client-visible lenient-policy warnings (see
 *                         {@link SkipWarnings}) raised while reading. {@code null} means the reader
 *                         should fall back to emitting warnings directly via {@link
 *                         org.elasticsearch.common.logging.HeaderWarning}, which is only correct when
 *                         the read runs on the request/driver thread. Callers that dispatch reads to a
 *                         background thread (e.g. {@code AsyncExternalSourceOperatorFactory}) must set
 *                         this to a sink — typically {@code AsyncExternalSourceBuffer::recordInformationalWarning},
 *                         preserving these warnings' pre-existing behavior of never flipping the response's
 *                         {@code is_partial} flag (see {@code AsyncExternalSourceBuffer#recordWarning} for
 *                         the one warning that does) — so the warning is relayed back and re-emitted on
 *                         the correct thread instead of being silently dropped.
 * @param fileHeaderColumns the file's own column names, in file order, read from its leading bytes.
 *                         {@code null} for every read that owns the file's start, and for formats that do
 *                         not name their columns in a header. Set only for a read that cannot see the
 *                         header but still has to know what the columns are called — a chunk after the
 *                         first of a header-bearing file whose declared schema binds by name. Binding such
 *                         a chunk by position instead would shift every column silently.
 */
public record FormatReadContext(
    List<String> projectedColumns,
    int batchSize,
    int rowLimit,
    ErrorPolicy errorPolicy,
    boolean firstSplit,
    boolean lastSplit,
    boolean recordAligned,
    @Nullable List<Attribute> readSchema,
    long splitStartByte,
    int maxRecordBytes,
    long statsBaseOffset,
    long statsStripeSize,
    boolean statsFileFinal,
    StripeColumnScope statsColumnScope,
    @Nullable Consumer<String> informationalWarningSink,
    @Nullable List<String> fileHeaderColumns
) {

    public FormatReadContext {
        if (readSchema != null && readSchema.isEmpty()) {
            readSchema = null;
        }
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("maxRecordBytes must be positive, got: " + maxRecordBytes);
        }
        if (statsColumnScope == null) {
            statsColumnScope = StripeColumnScope.PROJECTED;
        }
    }

    /**
     * Creates a minimal context for the common non-split case. Leaves {@code errorPolicy} as
     * {@code null} so the reader falls back to its own default — typically the policy resolved
     * from the user's {@code WITH} options via {@link FormatReader#withConfig}, or the
     * {@link FormatReader#defaultErrorPolicy()} when no user options are set. Callers that need
     * to override the policy should use {@link #builder()} or {@link #withErrorPolicy(ErrorPolicy)}.
     */
    public static FormatReadContext of(List<String> projectedColumns, int batchSize) {
        return builder().projectedColumns(projectedColumns).batchSize(batchSize).build();
    }

    /**
     * Returns a copy with a different row limit.
     */
    public FormatReadContext withRowLimit(int limit) {
        return new FormatReadContext(
            projectedColumns,
            batchSize,
            limit,
            errorPolicy,
            firstSplit,
            lastSplit,
            recordAligned,
            readSchema,
            splitStartByte,
            maxRecordBytes,
            statsBaseOffset,
            statsStripeSize,
            statsFileFinal,
            statsColumnScope,
            informationalWarningSink,
            fileHeaderColumns
        );
    }

    /**
     * Returns a copy with a different error policy.
     */
    public FormatReadContext withErrorPolicy(ErrorPolicy policy) {
        return new FormatReadContext(
            projectedColumns,
            batchSize,
            rowLimit,
            policy,
            firstSplit,
            lastSplit,
            recordAligned,
            readSchema,
            splitStartByte,
            maxRecordBytes,
            statsBaseOffset,
            statsStripeSize,
            statsFileFinal,
            statsColumnScope,
            informationalWarningSink,
            fileHeaderColumns
        );
    }

    /**
     * Returns a copy configured for a split-based read.
     */
    public FormatReadContext withSplit(boolean first, boolean last) {
        return new FormatReadContext(
            projectedColumns,
            batchSize,
            rowLimit,
            errorPolicy,
            first,
            last,
            recordAligned,
            readSchema,
            splitStartByte,
            maxRecordBytes,
            statsBaseOffset,
            statsStripeSize,
            statsFileFinal,
            statsColumnScope,
            informationalWarningSink,
            fileHeaderColumns
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<String> projectedColumns;
        private int batchSize;
        private int rowLimit = FormatReader.NO_LIMIT;
        // Defaults to null so the reader falls back to its own resolved policy
        // (typically the WITH-options policy from withConfig). Callers that want to override
        // explicitly should call errorPolicy(...).
        private ErrorPolicy errorPolicy = null;
        private boolean firstSplit = true;
        private boolean lastSplit = true;
        private boolean recordAligned = false;
        @Nullable
        private List<Attribute> readSchema = null;
        private long splitStartByte = 0L;
        private int maxRecordBytes = SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES;
        private long statsBaseOffset = 0L;
        private long statsStripeSize = -1L;
        @Nullable
        private List<String> fileHeaderColumns = null;
        private boolean statsFileFinal = false;
        private StripeColumnScope statsColumnScope = StripeColumnScope.PROJECTED;
        @Nullable
        private Consumer<String> informationalWarningSink = null;

        private Builder() {}

        public Builder projectedColumns(List<String> projectedColumns) {
            this.projectedColumns = projectedColumns;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder rowLimit(int rowLimit) {
            this.rowLimit = rowLimit;
            return this;
        }

        public Builder errorPolicy(ErrorPolicy errorPolicy) {
            this.errorPolicy = errorPolicy;
            return this;
        }

        public Builder firstSplit(boolean firstSplit) {
            this.firstSplit = firstSplit;
            return this;
        }

        public Builder lastSplit(boolean lastSplit) {
            this.lastSplit = lastSplit;
            return this;
        }

        /**
         * Marks the split as starting at a record boundary so line-oriented readers can skip the
         * "trim leading partial record" workaround used for byte-range macro-splits.
         */
        public Builder recordAligned(boolean recordAligned) {
            this.recordAligned = recordAligned;
            return this;
        }

        /** See {@link FormatReadContext#readSchema()}; pass {@code null} to fall back to per-file inference. */
        public Builder readSchema(@Nullable List<Attribute> readSchema) {
            this.readSchema = readSchema;
            return this;
        }

        /** See {@link FormatReadContext#splitStartByte()}; the file-global byte offset of this split's start. */
        public Builder splitStartByte(long splitStartByte) {
            this.splitStartByte = splitStartByte;
            return this;
        }

        public Builder maxRecordBytes(int maxRecordBytes) {
            this.maxRecordBytes = maxRecordBytes;
            return this;
        }

        /**
         * Canonical-stripe addressing for per-stripe stats capture: {@code baseOffset} is this read's
         * first byte in file/decompressed coordinates, {@code stripeSize} the grid ({@code <= 0}
         * disables), {@code fileFinal} whether this read reaches the file's true end (only the final
         * read may mark its last stripe complete + terminal). A pure stats overlay; never affects
         * chunking or splitting.
         */
        public Builder stats(long baseOffset, long stripeSize, boolean fileFinal) {
            this.statsBaseOffset = baseOffset;
            this.statsStripeSize = stripeSize;
            this.statsFileFinal = fileFinal;
            return this;
        }

        /**
         * Sets how much per-stripe statistics the read harvests (see {@link FormatReadContext#statsColumnScope()}).
         * {@code null} restores the {@link StripeColumnScope#PROJECTED} default. Orthogonal to {@link #stats}.
         */
        public Builder statsColumnScope(@Nullable StripeColumnScope statsColumnScope) {
            this.statsColumnScope = statsColumnScope != null ? statsColumnScope : StripeColumnScope.PROJECTED;
            return this;
        }

        /**
         * See {@link FormatReadContext#informationalWarningSink()}; pass {@code null} for
         * direct-to-HeaderWarning emission.
         */
        public Builder informationalWarningSink(@Nullable Consumer<String> informationalWarningSink) {
            this.informationalWarningSink = informationalWarningSink;
            return this;
        }

        /**
         * The file's own column names, in file order, read from its leading bytes.
         * <p>
         * Only set for a read that does NOT own the file's start but still needs to know what its columns
         * are called — a chunk after the first of a header-bearing file whose declared schema binds by name.
         * Such a chunk cannot see the header itself, and binding by position instead would silently shift
         * every column. The component that cut the file into chunks reads the header once and states it here.
         */
        public Builder fileHeaderColumns(@Nullable List<String> fileHeaderColumns) {
            this.fileHeaderColumns = fileHeaderColumns;
            return this;
        }

        public FormatReadContext build() {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
            }
            return new FormatReadContext(
                projectedColumns,
                batchSize,
                rowLimit,
                errorPolicy,
                firstSplit,
                lastSplit,
                recordAligned,
                readSchema,
                splitStartByte,
                maxRecordBytes,
                statsBaseOffset,
                statsStripeSize,
                statsFileFinal,
                statsColumnScope,
                informationalWarningSink,
                fileHeaderColumns
            );
        }
    }
}
