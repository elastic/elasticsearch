/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A plugin that provides a script language "pause" that can be used to simulate slow running queries.
 * This implementation allows to know when it arrives at execute() via startEmitting and to allow the execution to proceed
 * via allowEmitting.
 */
public class SimplePauseFieldPlugin extends AbstractPauseFieldPlugin {
    public static CountDownLatch startEmitting = new CountDownLatch(1);
    public static CountDownLatch allowEmitting = new CountDownLatch(1);

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
        try {
            return allowEmitting.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return true;
        }
    }
}
