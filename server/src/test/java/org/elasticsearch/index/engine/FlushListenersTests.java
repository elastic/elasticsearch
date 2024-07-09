/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class FlushListenersTests extends ESTestCase {

    public void testFlushListenerCompletedImmediatelyIfFlushAlreadyOccurred() {
        try (FlushListeners flushListeners = new FlushListeners(logger, new ThreadContext(Settings.EMPTY))) {
            long generation = randomLongBetween(1, 10);
            Translog.Location lastWriteLocation = new Translog.Location(
                randomLongBetween(3, 15),
                randomLongBetween(100, 800),
                Integer.MAX_VALUE
            );
            flushListeners.afterFlush(generation, lastWriteLocation);
            Translog.Location waitLocation = new Translog.Location(
                lastWriteLocation.generation() - randomLongBetween(0, 2),
                lastWriteLocation.generation() - randomLongBetween(10, 90),
                2
            );
            PlainActionFuture<Long> future = new PlainActionFuture<>();
            flushListeners.addOrNotify(waitLocation, future);
            assertThat(future.actionGet(), equalTo(generation));
        }
    }

    public void testFlushListenerCompletedAfterLocationFlushed() {
        try (FlushListeners flushListeners = new FlushListeners(logger, new ThreadContext(Settings.EMPTY))) {
            long generation = randomLongBetween(1, 10);
            Translog.Location lastWriteLocation = new Translog.Location(
                randomLongBetween(3, 15),
                randomLongBetween(100, 800),
                Integer.MAX_VALUE
            );
            Translog.Location waitLocation = new Translog.Location(
                lastWriteLocation.generation() - randomLongBetween(0, 2),
                lastWriteLocation.generation() - randomLongBetween(10, 90),
                2
            );
            PlainActionFuture<Long> future = new PlainActionFuture<>();
            flushListeners.addOrNotify(waitLocation, future);
            assertFalse(future.isDone());

            flushListeners.afterFlush(generation, lastWriteLocation);
            assertThat(future.actionGet(), equalTo(generation));

            long generation2 = generation + 1;
            Translog.Location secondLastWriteLocation = new Translog.Location(
                lastWriteLocation.generation(),
                lastWriteLocation.translogLocation() + 10,
                Integer.MAX_VALUE
            );
            Translog.Location waitLocation2 = new Translog.Location(
                lastWriteLocation.generation(),
                lastWriteLocation.translogLocation() + 4,
                2
            );

            PlainActionFuture<Long> future2 = new PlainActionFuture<>();
            flushListeners.addOrNotify(waitLocation2, future2);
            assertFalse(future2.isDone());

            flushListeners.afterFlush(generation2, secondLastWriteLocation);
            assertThat(future2.actionGet(), equalTo(generation2));
        }
    }

    public void testFlushListenerClose() {
        PlainActionFuture<Long> future = new PlainActionFuture<>();
        try (FlushListeners flushListeners = new FlushListeners(logger, new ThreadContext(Settings.EMPTY))) {
            Translog.Location waitLocation = new Translog.Location(randomLongBetween(0, 2), randomLongBetween(10, 90), 2);
            flushListeners.addOrNotify(waitLocation, future);
            assertFalse(future.isDone());

            flushListeners.close();

            expectThrows(AlreadyClosedException.class, future::actionGet);

            expectThrows(IllegalStateException.class, () -> flushListeners.addOrNotify(waitLocation, new PlainActionFuture<>()));
        }
    }
}
