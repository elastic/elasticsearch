/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link SourceOperator} that replays an already-materialized list of {@link Page}s one at a time, so a coordinator {@code Driver} can
 * post-process pages the compute produced (e.g. {@code ExpandUnmappedFieldsOperator}). Emitting one page per {@code getOutput} lets the
 * driver yield between pages.
 *
 * <p>The operator takes ownership of the pages: each is handed downstream by {@link #getOutput} and its slot nulled, so {@link #close}
 * releases only the pages not yet emitted (e.g. after early termination). Page release is idempotent regardless.
 */
final class PageListSourceOperator extends SourceOperator {
    private final List<Page> pages;
    private int next = 0;
    private boolean finished = false;

    PageListSourceOperator(List<Page> pages) {
        this.pages = new ArrayList<>(pages);
    }

    @Override
    public Page getOutput() {
        if (finished || next >= pages.size()) {
            return null;
        }
        Page page = pages.get(next);
        pages.set(next, null);
        next++;
        return page;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished || next >= pages.size();
    }

    @Override
    public void close() {
        List<Page> remaining = new ArrayList<>(pages);
        pages.clear();
        Releasables.closeExpectNoException(remaining);
    }

    @Override
    public String toString() {
        return "PageListSourceOperator[pages=" + pages.size() + "]";
    }
}
