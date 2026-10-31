/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

import java.util.Optional;

/**
 * Joins many "right hand" pages against one "left hand" {@link Page}. Each "right hand" page starts with a
 * non-decreasing {@code positions} column pointing into the "left hand" page, followed by the right columns.
 * Implementations differ in what they do with "left hand" rows that no "right hand" row points at.
 */
public interface RightChunkedJoin extends Releasable {
    /**
     * Join one "right hand" page. The caller keeps ownership of {@code rightHand}.
     */
    Page join(Page rightHand);

    /**
     * Called once all "right hand" pages were joined, returns the trailing output, if any.
     */
    Optional<Page> noMoreRightHandPages();

    /**
     * Release this on <strong>any</strong> thread, rather than just the thread that built it.
     */
    void releaseOnAnyThread();
}
