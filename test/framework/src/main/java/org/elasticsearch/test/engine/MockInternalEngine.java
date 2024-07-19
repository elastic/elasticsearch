/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.engine;

import org.apache.lucene.index.FilterDirectoryReader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngine;

import java.io.IOException;
import java.util.function.Function;

final class MockInternalEngine extends InternalEngine {
    private MockEngineSupport support;
    private Class<? extends FilterDirectoryReader> wrapperClass;

    MockInternalEngine(EngineConfig config, Class<? extends FilterDirectoryReader> wrapper) throws EngineException {
        super(config);
        wrapperClass = wrapper;

    }

    private synchronized MockEngineSupport support() {
        // lazy initialized since we need it already on super() ctor execution :(
        if (support == null) {
            support = new MockEngineSupport(config(), wrapperClass);
        }
        return support;
    }

    @Override
    public void close(ActionListener<Void> listener) throws IOException {
        switch (support().flushOrClose(MockEngineSupport.CloseAction.CLOSE)) {
            case FLUSH_AND_CLOSE -> flushAndCloseInternal(listener);
            case CLOSE -> super.close(listener);
        }
    }

    @Override
    public void flushAndClose(ActionListener<Void> listener) throws IOException {
        switch (support().flushOrClose(MockEngineSupport.CloseAction.FLUSH_AND_CLOSE)) {
            case FLUSH_AND_CLOSE -> flushAndCloseInternal(listener);
            case CLOSE -> super.close(listener);
        }
    }

    private void flushAndCloseInternal(ActionListener<Void> listener) throws IOException {
        if (support().isFlushOnCloseDisabled() == false) {
            super.flushAndClose(listener);
        } else {
            super.close(listener);
        }
    }

    @Override
    public Engine.Searcher acquireSearcher(String source, SearcherScope scope) {
        final Engine.Searcher engineSearcher = super.acquireSearcher(source, scope);
        return support().wrapSearcher(engineSearcher);
    }

    @Override
    public SearcherSupplier acquireSearcherSupplier(Function<Searcher, Searcher> wrapper, SearcherScope scope) throws EngineException {
        return super.acquireSearcherSupplier(wrapper.andThen(s -> support().wrapSearcher(s)), scope);
    }
}
