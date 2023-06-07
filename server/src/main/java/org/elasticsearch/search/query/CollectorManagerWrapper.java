/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;

import java.io.IOException;
import java.util.Collection;

public class CollectorManagerWrapper<C extends Collector> implements CollectorManager<C, Void> {

    private final CollectorManager<C, Void> wrapped;
    private C collector;

    public CollectorManagerWrapper(CollectorManager<C, Void> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public C newCollector() throws IOException {
        //TODO make it single threaded and assert on it?
        C collector = wrapped.newCollector();
        this.collector = collector;
        return collector;
    }

    public C getCollector() {
        return collector;
    }

    @Override
    public Void reduce(Collection<C> collectors) throws IOException {
        //TODO add assertions?
        return wrapped.reduce(collectors);
    }
}
