/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.utils;

import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulates a common pattern of trying to open a bunch of resources and then transferring ownership elsewhere on success,
 * but closing them on failure.
 */
public class TransferableCloseables implements Closeable {

    private boolean transferred = false;
    private final List<Closeable> closeables = new ArrayList<>();

    public <T extends Closeable> T add(T releasable) {
        assert transferred == false : "already transferred";
        closeables.add(releasable);
        return releasable;
    }

    public Closeable transfer() {
        assert transferred == false : "already transferred";
        transferred = true;
        Collections.reverse(closeables);
        return () -> IOUtils.close(closeables);
    }

    @Override
    public void close() throws IOException {
        if (transferred == false) {
            IOUtils.close(closeables);
        }
    }
}
