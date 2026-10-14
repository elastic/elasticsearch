/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Collects the coordinator's output pages, optionally failing as soon as they exceed a size limit.
 * <p>
 * A sub-plan whose result is materialized into a local relation (the right side of a {@code JOIN (subquery)}) is
 * checked against {@code esql.intermediate_local_relation_max_size} once it completes. Without a limit here the
 * sub-plan would read its whole input and buffer every page on the coordinator before that check could fail.
 * The size is measured with {@link Page#ramBytesUsedByBlocks()}, the same metric as the post-execution check, so
 * a result fails here exactly when it would have failed there, only earlier.
 */
final class BoundedPageCollector implements Consumer<Page> {
    static final String TOO_LARGE_MESSAGE_PREFIX = "sub-plan execution results too large";

    private final List<Page> pages = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong collectedBytes = new AtomicLong();
    private final ByteSizeValue maxSize;

    /**
     * @param maxSize the limit on the collected pages, or {@code null} to collect without a limit
     */
    BoundedPageCollector(ByteSizeValue maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * The collected pages. Pages are added before the limit is checked, so a page that crosses the limit is still
     * in this list and is released together with the others by the failure path.
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
                    + "; narrow the subquery or increase ["
                    + PlannerSettings.INTERMEDIATE_LOCAL_RELATION_MAX_SIZE.getKey()
                    + "]"
            );
        }
    }
}
