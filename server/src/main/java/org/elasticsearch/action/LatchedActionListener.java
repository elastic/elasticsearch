/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import java.util.concurrent.CountDownLatch;

/**
 * An action listener that allows passing in a {@link CountDownLatch} that
 * will be counted down after onResponse or onFailure is called
 */
public class LatchedActionListener<T> implements ActionListener<T> {

    private final ActionListener<T> delegate;
    private final CountDownLatch latch;

    public LatchedActionListener(ActionListener<T> delegate, CountDownLatch latch) {
        this.delegate = delegate;
        this.latch = latch;
    }

    @Override
    public void onResponse(T t) {
        try {
            delegate.onResponse(t);
        } finally {
            latch.countDown();
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            delegate.onFailure(e);
        } finally {
            latch.countDown();
        }
    }
}
