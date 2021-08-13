/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index;

import org.apache.lucene.index.AssertingDirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.engine.MockEngineFactory;
import org.elasticsearch.test.engine.MockEngineSupport;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * A plugin to use {@link MockEngineFactory}.
 *
 * Subclasses may override the reader wrapper used.
 */
public class MockEngineFactoryPlugin extends Plugin implements EnginePlugin {

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE, MockEngineSupport.WRAP_READER_RATIO);
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
        return Optional.of(new MockEngineFactory(getReaderWrapperClass()));
    }

    protected Class<? extends FilterDirectoryReader> getReaderWrapperClass() {
        return AssertingDirectoryReader.class;
    }
}
