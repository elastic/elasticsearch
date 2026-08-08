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

    @Nullable
    private final FieldContextResolver resolver;
    private final int primarySortFieldNumber;
    private final int maxDoc;

    RunTableGate(@Nullable FieldContextResolver resolver, int primarySortFieldNumber, int maxDoc) {
        this.resolver = resolver;
        this.primarySortFieldNumber = primarySortFieldNumber;
        this.maxDoc = maxDoc;
    }

    /**
     * Returns {@code true} when run-table ordinal encoding should be attempted for this field.
     *
     * <p>Returns {@code false} -- meaning fall back immediately, without starting the doc walk --
     * when any of the following holds:
     * <ul>
     *   <li>the field is the primary sort field (uses ordinal range encoding instead)</li>
     *   <li>the resolver is {@code null} (run-table ordinal feature is disabled)</li>
     *   <li>the field is not a TSDB dimension</li>
     *   <li>{@code maxOrd * 2 > maxDoc}: the minimum run count ({@code maxOrd}, one run per unique
     *       value in a perfectly sorted segment) already exceeds the threshold, so no doc walk
     *       can change the outcome</li>
     * </ul>
     */
    boolean allow(FieldInfo field, long maxOrd) {
        if (field.number == primarySortFieldNumber) {
            return false;
        }
        if (resolver == null || resolver.resolve(field.name, 0).isDimension() == false) {
            return false;
        }
        return maxOrd * 2 <= maxDoc;
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
        return (processedDocs << 3) < maxDoc || (long) numRuns * 2 <= processedDocs;
    }
}
