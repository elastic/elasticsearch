/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public abstract class TransformSingleNodeTestCase extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(LocalStateTransform.class, ReindexPlugin.class);
    }

    protected <T> void assertAsync(Consumer<ActionListener<T>> function, T expected, CheckedConsumer<T, ? extends Exception> onAnswer,
            Consumer<Exception> onException) throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            if (expected == null) {
                fail("expected an exception but got a response");
            } else {
                assertThat(r, equalTo(expected));
            }
            if (onAnswer != null) {
                onAnswer.accept(r);
            }
        }, e -> {
            if (onException == null) {
                logger.error("got unexpected exception", e);
                fail("got unexpected exception: " + e.getMessage());
            } else {
                onException.accept(e);
            }
        }), latch);

        function.accept(listener);
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }

}
