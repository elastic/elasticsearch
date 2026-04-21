/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

/**
 * Utility for draining pages from a {@link CloseableIterator} into an {@link AsyncExternalSourceBuffer}
 * with backpressure. Uses blocking wait instead of spin-wait, relying on the buffer's
 * {@code notifyNotFull()} in {@code finish()} to wake producers when no more input is needed.
 *
 * <p>Buffer-space blocking uses {@link AsyncExternalSourceBuffer#awaitSpaceForProducer} (a timed condition wait
 * on the buffer's not-full lock), not {@link org.elasticsearch.action.support.PlainActionFuture}, so a
 * producer and a consumer on different threads of the same named pool (e.g. {@code esql_worker}) do not
 * trip {@code PlainActionFuture}'s same-pool completion assertion.
 */
public final class ExternalSourceDrainUtils {

    static final TimeValue DEFAULT_DRAIN_TIMEOUT = TimeValue.timeValueMinutes(5);

    private ExternalSourceDrainUtils() {}

    public static void drainPages(CloseableIterator<Page> pages, AsyncExternalSourceBuffer buffer) {
        drainPages(pages, buffer, DEFAULT_DRAIN_TIMEOUT);
    }

    public static void drainPages(CloseableIterator<Page> pages, AsyncExternalSourceBuffer buffer, TimeValue timeout) {
        while (pages.hasNext() && buffer.noMoreInputs() == false) {
            buffer.awaitSpaceForProducer(timeout);
            if (buffer.noMoreInputs()) {
                break;
            }
            Page page = pages.next();
            page.allowPassingToDifferentDriver();
            buffer.addPage(page);
        }
    }

    public static int drainPagesWithBudget(CloseableIterator<Page> pages, AsyncExternalSourceBuffer buffer) {
        return drainPagesWithBudget(pages, buffer, FormatReader.NO_LIMIT, DEFAULT_DRAIN_TIMEOUT);
    }

    public static int drainPagesWithBudget(CloseableIterator<Page> pages, AsyncExternalSourceBuffer buffer, int rowLimit) {
        return drainPagesWithBudget(pages, buffer, rowLimit, DEFAULT_DRAIN_TIMEOUT);
    }

    public static int drainPagesWithBudget(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        int rowLimit,
        TimeValue timeout
    ) {
        int totalRows = 0;
        while (pages.hasNext() && buffer.noMoreInputs() == false) {
            if (rowLimit != FormatReader.NO_LIMIT && totalRows >= rowLimit) {
                break;
            }
            buffer.awaitSpaceForProducer(timeout);
            if (buffer.noMoreInputs()) {
                break;
            }
            Page page = pages.next();
            totalRows += page.getPositionCount();
            page.allowPassingToDifferentDriver();
            buffer.addPage(page);
        }
        return totalRows;
    }
}
