/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectionKey;

public class TestSelectionKey extends AbstractSelectionKey {

    private int interestOps = 0;
    private int readyOps;

    public TestSelectionKey(int ops) {
        this.interestOps = ops;
    }

    @Override
    public SelectableChannel channel() {
        return null;
    }

    @Override
    public Selector selector() {
        return null;
    }

    @Override
    public int interestOps() {
        return interestOps;
    }

    @Override
    public SelectionKey interestOps(int ops) {
        this.interestOps = ops;
        return this;
    }

    @Override
    public int readyOps() {
        return readyOps;
    }

    public void setReadyOps(int readyOps) {
        this.readyOps = readyOps;
    }
}
