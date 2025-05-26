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

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@LuceneTestCase.SuppressCodecs("*") // only use our own codecs
public class DirectIOIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class);
    }

    private void indexVectors() {
        assertAcked(
            prepareCreate("foo-vectors").setSettings(Settings.builder().put(InternalSettingsPlugin.USE_COMPOUND_FILE.getKey(), false))
                .setMapping("""
                    {
                      "properties": {
                        "fooVector": {
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
        );
        ensureGreen("foo-vectors");

        for (int i = 0; i < 1000; i++) {
            indexDoc("foo-vectors", Integer.toString(i), "fooVector", IntStream.range(0, 64).mapToDouble(d -> randomFloat()).toArray());
        }
        refresh();
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
            var knn = List.of(new KnnSearchBuilder("fooVector", new VectorData(null, new byte[64]), 10, 20, null, null));
            assertHitCount(prepareSearch("foo-vectors").setKnnSearch(knn), 10);
            mockLog.assertAllExpectationsMatched();
        }
    }

    @Override
    protected boolean addMockFSIndexStore() {
        return false; // we require to always use the "real" hybrid directory
    }
}
