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
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;

/**
 * Decides, for one segment, which fields are written in the run-table ordinal layout.
 *
 * <p>{@link #allow(FieldInfo, long)} runs before the doc walk and admits only fields whose ordinal stream
 * is expected to be run-shaped. {@link #allow(int, int)} runs during the walk and gives up as soon as the
 * runs seen so far show the table will not be compact enough. Either returning {@code false} means the
 * field falls back to the baseline layout.
 */
final class RunTableGate {

    /**
     * Upper bound on the runs of one field. The writer collects runs on-heap before encoding them, two ints
     * per run, so this bounds that memory independently of the segment size.
     * TODO: stream the runs to a temp file or BigArrays instead, so that large segments of short series can
     * still use the layout without holding the runs on-heap.
     */
    static final int MAX_RUNS = 1 << 22;

    @Nullable
    private final FieldContextResolver resolver;
    private final int primarySortFieldNumber;
    private final int maxDoc;
    private final int blockSize;

    RunTableGate(@Nullable final FieldContextResolver resolver, int primarySortFieldNumber, int maxDoc, int blockSize) {
        this.resolver = resolver;
        this.primarySortFieldNumber = primarySortFieldNumber;
        this.maxDoc = maxDoc;
        this.blockSize = blockSize;
    }

    /**
     * Whether to attempt the run-table layout for {@code field}. Returns {@code false} when:
     * <ul>
     *   <li>the field is the primary sort field, which has its own ordinal-range layout;</li>
     *   <li>there is no resolver, which is how a read-only format instance disables the layout;</li>
     *   <li>the field is neither a dimension nor {@code _ts_routing_hash};</li>
     *   <li>{@code maxOrd <= 1}: the baseline layout already stores a single-valued field as a constant, which
     *       readers can load in bulk;</li>
     *   <li>{@code maxOrd * 2 > maxDoc}: even one run per value would already exceed the size budget.</li>
     * </ul>
     */
    boolean allow(final FieldInfo field, long maxOrd) {
        if (field.number == primarySortFieldNumber) {
            return false;
        }
        // The dimension check is a policy, not a correctness requirement: the mid-walk check measures the
        // actual run density for any field. It keeps the fallback rate, which costs a second pass over the
        // values, near zero, because dimensions are constant within a series and segments are sorted by series.
        // _ts_routing_hash is a metadata field, so it is not a dimension, but it is constant per series too.
        if (resolver == null
            || (field.name.equals(TimeSeriesRoutingHashFieldMapper.NAME) == false
                && resolver.resolve(field.name, blockSize).isDimension() == false)) {
            return false;
        }
        return maxOrd > 1 && maxOrd * 2 <= maxDoc;
    }

    /**
     * Whether to keep building the run table after {@code processedDocs} docs produced {@code numRuns} runs.
     * Gives up once the runs exceed {@link #MAX_RUNS} or the size budget (an average run must span at least two
     * docs), or, after the first eighth of the segment, once the run density seen so far is already over that
     * budget.
     */
    boolean allow(int numRuns, int processedDocs) {
        if (numRuns > MAX_RUNS || (long) numRuns * 2 > maxDoc) {
            return false;
        }
        return (long) processedDocs * 8 < maxDoc || (long) numRuns * 2 <= processedDocs;
    }
}
