/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.translog.ChannelFactory;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class BulkAfterWriteFsyncFailureIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestEnginePlugin.class);
    }

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {
        static final AtomicBoolean simulateFsyncFailure = new AtomicBoolean(false);

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> new InternalEngine(config) {
                @Override
                protected ChannelFactory getTranslogChannelFactory() {
                    return (file, openOption) -> new FilterFileChannel(FileChannel.open(file, openOption)) {
                        @Override
                        public void force(boolean metaData) throws IOException {
                            if (simulateFsyncFailure.get()) {
                                throw new IOException("Simulated failure");
                            } else {
                                super.force(metaData);
                            }
                        }
                    };
                }
            });
        }
    }

    public void testFsyncFailure() {
        internalCluster().startDataOnlyNode();
        String indexName = randomIdentifier();
        createIndex(indexName);
        ensureGreen(indexName);
        TestEnginePlugin.simulateFsyncFailure.set(true);
        var response = indexDoc(indexName, "1", "field", "foo");
    }
}
