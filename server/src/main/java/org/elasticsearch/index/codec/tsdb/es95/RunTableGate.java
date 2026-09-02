/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

/**
 * Two-gate filter for the run-table ordinal layout.
 *
 * <p>The first gate ({@link #allow(FieldInfo, long)}) is a static pre-write check: it opens
 * only for fields where run-table encoding can plausibly be beneficial before the doc walk
 * starts. The second gate ({@link #allow(int, int)}) is a dynamic mid-accumulation check: it
 * closes once the accumulation state shows the run table will exceed the size budget. Both gates
 * return {@code false} to signal fall back to the baseline layout; if the first gate closes, the
 * second is never reached.
 */
final class RunTableGate {

    // Temporary switch for the _tsid run-table benchmark. Leave true for the A run; flip to false in a
    // separate commit for the B run to route _tsid back to the ordinal-range layout while dimension
    // run-table stays on. Remove once the storage and indexing tradeoff is settled.
    private static final boolean TSID_RUN_TABLE_ENABLED = false;

    @Nullable
    private final FieldContextResolver resolver;
    private final int primarySortFieldNumber;
    private final int maxDoc;
    private final int blockSize;

    RunTableGate(@Nullable FieldContextResolver resolver, int primarySortFieldNumber, int maxDoc, int blockSize) {
        this.resolver = resolver;
        this.primarySortFieldNumber = primarySortFieldNumber;
        this.maxDoc = maxDoc;
        this.blockSize = blockSize;
    }

    /**
     * Returns {@code true} when run-table ordinal encoding should be attempted for this field.
     *
     * <p>Returns {@code false} -- meaning fall back immediately, without starting the doc walk --
     * when any of the following holds:
     * <ul>
     *   <li>the resolver is {@code null} (run-table ordinal feature is disabled)</li>
     *   <li>the field is the primary sort field (uses ordinal range encoding instead), unless it is
     *       {@code _tsid}, which is run-shaped by construction and takes the run-table layout directly</li>
     *   <li>the field is neither {@code _tsid} nor a TSDB dimension</li>
     *   <li>{@code maxOrd * 2 > maxDoc}: the minimum run count ({@code maxOrd}, one run per unique
     *       value in a perfectly sorted segment) already exceeds the threshold, so no doc walk
     *       can change the outcome</li>
     * </ul>
     */
    boolean allow(FieldInfo field, long maxOrd) {
        if (TSID_RUN_TABLE_ENABLED && resolver != null && TimeSeriesIdFieldMapper.NAME.equals(field.name)) {
            return maxOrd > 0 && maxOrd * 2 <= maxDoc;
        }
        if (field.number == primarySortFieldNumber) {
            return false;
        }
        // NOTE: The isDimension() check is a policy gate, not a correctness requirement. Both
        // mid-scan checks are structurally general: they measure actual run density and bail early
        // when the ordinal stream is not run-shaped, regardless of field type or index mode. Any
        // sorted index whose non-primary-sort fields cluster by value (logs sorted by
        // host+@timestamp, any custom-sorted keyword index) exhibits the same piecewise-constant
        // structure the run table exploits. This gate exists because without it every sorted field
        // starts a doc walk, paying a re-read fallback cost when ordinals are not run-shaped and
        // adding a per-field discriminator byte regardless. Restricting to TSDB dimensions keeps
        // the fallback rate near zero because those fields are structurally guaranteed to be
        // run-shaped in a TSDB segment. To generalize, replace this check with a broader pre-walk
        // signal (cardinality-to-doc ratio, index sort configuration, or field-level statistics)
        // that bounds the fallback rate without relying on the TSDB dimension predicate.
        if (resolver == null || resolver.resolve(field.name, blockSize).isDimension() == false) {
            return false;
        }
        return maxOrd > 0 && maxOrd * 2 <= maxDoc;
    }

    /**
     * Returns {@code false} during the doc walk when the run table should be abandoned.
     *
     * <p>Two conditions close this gate:
     * <ol>
     *   <li>Absolute: {@code numRuns * 2 > maxDoc} -- even in the best case (all remaining docs
     *       extend existing runs), the final run table exceeds the size threshold.</li>
     *   <li>Projected: once at least 1/8 of {@code maxDoc} docs have been processed, close when
     *       the current run density already exceeds the threshold for the sample seen so far.
     *       This catches badly-sorted fields early and avoids walking the remaining docs.</li>
     * </ol>
     *
     * @param numRuns       run count accumulated so far
     * @param processedDocs number of docs walked so far ({@code doc + 1} in a zero-based loop)
     */
    boolean allow(int numRuns, int processedDocs) {
        if ((long) numRuns * 2 > maxDoc) {
            return false;
        }
        return (long) processedDocs * 8 < maxDoc || (long) numRuns * 2 <= processedDocs;
    }
}
