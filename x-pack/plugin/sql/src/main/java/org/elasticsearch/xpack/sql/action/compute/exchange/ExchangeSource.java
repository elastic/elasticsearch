/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.exchange;

import org.elasticsearch.xpack.sql.action.compute.Page;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class ExchangeSource {

    private final BlockingQueue<Page> buffer = new LinkedBlockingDeque<>();

    private volatile boolean finishing;

    public ExchangeSource() {

    }


    public void addPage(Page page) {
        synchronized (this) {
            // ignore pages after finish
            if (finishing == false) {
                buffer.add(page);
            }
        }
    }

    public Page removePage() {
        Page page = buffer.poll();
        return page;
    }

    public boolean isFinished() {
        if (finishing == false) {
            return false;
        }
        synchronized (this) {
            return finishing && buffer.isEmpty();
        }
    }

    public void finish() {
        synchronized (this) {
            if (finishing) {
                return;
            }
            finishing = true;
        }
    }
}
