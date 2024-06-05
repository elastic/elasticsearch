/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.engine;

import org.apache.lucene.index.FilterDirectoryReader;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;

public final class MockEngineFactory implements EngineFactory {

    private final Class<? extends FilterDirectoryReader> wrapper;

    public MockEngineFactory(Class<? extends FilterDirectoryReader> wrapper) {
        this.wrapper = wrapper;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        return new MockInternalEngine(config, wrapper);
    }
}
