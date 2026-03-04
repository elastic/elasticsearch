/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;

/**
 * Utility for draining pages from a {@link CloseableIterator} into an {@link AsyncExternalSourceBuffer}
 * with backpressure. Uses blocking wait instead of spin-wait, relying on the buffer's
 * {@code notifyNotFull()} in {@code finish()} to wake producers when no more input is needed.
 */
public final class ExternalSourceDrainUtils {

    private static final TimeValue DRAIN_TIMEOUT = TimeValue.timeValueMinutes(5);

    private ExternalSourceDrainUtils() {}

    public static void drainPages(CloseableIterator<Page> pages, AsyncExternalSourceBuffer buffer) {
        while (pages.hasNext() && buffer.noMoreInputs() == false) {
            var spaceListener = buffer.waitForSpace();
            if (spaceListener.isDone() == false) {
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                spaceListener.addListener(future);
                future.actionGet(DRAIN_TIMEOUT);
            }
            if (buffer.noMoreInputs()) {
                break;
            }
            Page page = pages.next();
            page.allowPassingToDifferentDriver();
            buffer.addPage(page);
        }
    }
}
