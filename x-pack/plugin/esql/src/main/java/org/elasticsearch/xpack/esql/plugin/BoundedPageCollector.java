/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Collects the result pages of a plan on the coordinator, failing as soon as they exceed a size limit.
 * <p>
 * The result of a sub-plan ({@code INLINE STATS}, {@code IN} subquery) becomes a local relation of the main plan, capped by
 * {@code esql.intermediate_local_relation_max_size}. The session checks that cap once the sub-plan has completed, so without a
 * limit here the coordinator would buffer the whole result, and a sub-plan that streams its input would read all of it, before
 * the query fails. The size is measured with {@link Page#ramBytesUsedByBlocks()}, like the session's check, so a result fails
 * here exactly when it would fail there, only earlier.
 */
final class BoundedPageCollector implements Consumer<Page> {
    static final String TOO_LARGE_MESSAGE_PREFIX = "sub-plan execution results too large";

    private final List<Page> pages = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong collectedBytes = new AtomicLong();
    @Nullable
    private final ByteSizeValue maxSize;

    /**
     * @param maxSize the limit on the collected pages, or {@code null} to collect without a limit
     */
    BoundedPageCollector(@Nullable ByteSizeValue maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * The collected pages. A page is added before the limit is checked, so the page that crosses the limit is in this list
     * too, and the caller's failure path releases it with the others.
     */
    List<Page> pages() {
        return pages;
    }

    @Override
    public void accept(Page page) {
        pages.add(page);
        if (maxSize == null) {
            return;
        }
        long total = collectedBytes.addAndGet(page.ramBytesUsedByBlocks());
        if (total > maxSize.getBytes()) {
            throw new IllegalArgumentException(
                TOO_LARGE_MESSAGE_PREFIX
                    + " [at least "
                    + ByteSizeValue.ofBytes(total)
                    + "] > "
                    + maxSize
                    + ", the limit set by ["
                    + PlannerSettings.INTERMEDIATE_LOCAL_RELATION_MAX_SIZE.getKey()
                    + "]"
            );
        }
    }
}
