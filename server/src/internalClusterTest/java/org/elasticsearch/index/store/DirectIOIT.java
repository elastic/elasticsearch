/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.logging.log4j.Level;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

@LuceneTestCase.SuppressCodecs("*") // only use our own codecs
public class DirectIOIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class);
    }

    private void indexVectors() {
        internalCluster().startNode();
        prepareCreate("vectors").setSettings(Settings.builder().put(InternalSettingsPlugin.USE_COMPOUND_FILE.getKey(), false))
            .setMapping("""
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 64,
                      "element_type": "float",
                      "index": true,
                      "similarity": "l2_norm",
                      "index_options": {
                        "type": "bbq_flat"
                      }
                    }
                  }
                }
                """)
            .get();
        ensureGreen("vectors");

        for (int i = 0; i < 1000; i++) {
            indexDoc("vectors", Integer.toString(i), "vector", IntStream.range(0, 64).mapToDouble(d -> randomFloat()).toArray());
        }
    }

    @TestLogging(value = "org.elasticsearch.index.store.FsDirectoryFactory:DEBUG", reason = "to capture trace logging for direct IO")
    public void testDirectIOUsed() {
        try (MockLog mockLog = MockLog.capture(FsDirectoryFactory.class)) {
            // we're just looking for some evidence direct IO is used
            mockLog.addExpectation(
                new MockLog.PatternSeenEventExpectation(
                    "Direct IO used",
                    FsDirectoryFactory.class.getCanonicalName(),
                    Level.DEBUG,
                    "Opening .*\\.vec with direct IO"
                )
            );

            indexVectors();

            // do a search
            prepareSearch("vectors").setKnnSearch(
                List.of(new KnnSearchBuilder("vector", new VectorData(null, new byte[64]), 10, 20, null, null))
            ).get();

            mockLog.assertAllExpectationsMatched();
        }
    }
}
