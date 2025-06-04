/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.exception.ElasticsearchException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A plugin that provides a script language "pause_fail" that can be used to make queries fail in a predictable way.
 */
public class FailingPauseFieldPlugin extends AbstractPauseFieldPlugin {
    public static CountDownLatch startEmitting = new CountDownLatch(1);
    public static CountDownLatch allowEmitting = new CountDownLatch(1);

    @Override
    protected String scriptTypeName() {
        return "pause_fail";
    }

    public static void resetPlugin() {
        allowEmitting = new CountDownLatch(1);
        startEmitting = new CountDownLatch(1);
    }

    public static void release() {
        allowEmitting.countDown();
    }

    @Override
    public void onStartExecute() {
        startEmitting.countDown();
    }

    @Override
    public boolean onWait() throws InterruptedException {
        allowEmitting.await(30, TimeUnit.SECONDS);
        throw new ElasticsearchException("Failing query");
    }
}
