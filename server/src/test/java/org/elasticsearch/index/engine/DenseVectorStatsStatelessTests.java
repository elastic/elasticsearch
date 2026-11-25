/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.NoMergePolicy;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class DenseVectorStatsStatelessTests extends EngineTestCase {

    @Override
    protected String defaultMapping() {
        return """
            {
              "properties": {
                "dv": {
                  "type": "dense_vector",
                  "dims": 3,
                  "similarity": "cosine"
                }
              }
            }
            """;
    }

    public void testCountsAreCollectedWhenNotStateless() throws Exception {
        final DenseVectorStats stats = denseVectorStats(Settings.EMPTY);
        assertThat(stats.getValueCount(), greaterThan(0L));
        assertThat(offHeapSize(stats), greaterThan(0L));
    }

    public void testCountsAreSkippedOnStatelessButOffHeapIsNot() throws Exception {
        final DenseVectorStats stats = denseVectorStats(Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build());
        assertEquals(0L, stats.getValueCount());
        assertThat(offHeapSize(stats), greaterThan(0L));
    }

    private static long offHeapSize(DenseVectorStats stats) {
        assertEquals(Set.of("dv"), stats.offHeapStats().keySet());
        return stats.offHeapStats().get("dv").values().stream().mapToLong(Long::longValue).sum();
    }

    /**
     * Indexes documents carrying a dense vector into an engine configured with the given node settings, and returns the
     * stats it reports for them.
     */
    private DenseVectorStats denseVectorStats(Settings nodeSettings) throws IOException {
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(defaultSettings.getIndex(), indexSettings(), nodeSettings);
        assertEquals(nodeSettings.isEmpty() == false, DiscoveryNode.isStateless(indexSettings.getNodeSettings()));

        try (Store store = createStore()) {
            final EngineConfig config = config(indexSettings, store, createTempDir(), NoMergePolicy.INSTANCE);
            final DocumentMapper documentMapper = config.getMapperService().documentMapper();
            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < randomIntBetween(4, 16); i++) {
                    final String source = Strings.format("{\"dv\":[%s,%s,%s]}", randomFloat() + 1, randomFloat() + 1, randomFloat() + 1);
                    engine.index(indexForDoc(documentMapper.parse(new SourceToParse("d_" + i, new BytesArray(source), XContentType.JSON))));
                }
                engine.refresh("test");
                return engine.denseVectorStats(config.getMapperService().mappingLookup());
            }
        }
    }
}
