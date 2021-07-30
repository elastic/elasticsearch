/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;

import java.util.concurrent.CountDownLatch;

public abstract class TransportWriteActionTestHelper {


    public static void performPostWriteActions(final IndexShard indexShard,
                                              final WriteRequest<?> request,
                                              @Nullable final Translog.Location location,
                                              final Logger logger) {
        final CountDownLatch latch = new CountDownLatch(1);
        TransportWriteAction.RespondingWriteResult writerResult = new TransportWriteAction.RespondingWriteResult() {
            @Override
            public void onSuccess(boolean forcedRefresh) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception ex) {
                throw new AssertionError(ex);
            }
        };
        new TransportWriteAction.AsyncAfterWriteAction(indexShard, request, location, writerResult, logger).run();
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
