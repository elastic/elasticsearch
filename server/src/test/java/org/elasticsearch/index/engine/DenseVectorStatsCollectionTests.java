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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies that {@link Engine#denseVectorStats} actually collects counts and off-heap sizes for a
 * dense_vector field, using a real {@link InternalEngine}. Counts and off-heap sizes both come from
 * field metadata via {@code KnnVectorsReader#getVectorCount}/{@code #getOffHeapByteSize}, so neither
 * opens the vector values; there is no stateless-specific code path left to test here. The stateless
 * variant below is kept as a regression guard against reintroducing a "skip counts on stateless"
 * shortcut, not because behaviour currently differs.
 */
public class DenseVectorStatsCollectionTests extends EngineTestCase {

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
        final int numDocs = randomIntBetween(4, 16);
        final DenseVectorStats stats = denseVectorStats(Settings.EMPTY, numDocs);
        assertThat(stats.getValueCount(), equalTo((long) numDocs));
        assertThat(offHeapSize(stats), greaterThan(0L));
    }

    public void testCountsAreCollectedOnStatelessToo() throws Exception {
        final int numDocs = randomIntBetween(4, 16);
        final Settings nodeSettings = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();
        final DenseVectorStats stats = denseVectorStats(nodeSettings, numDocs);
        assertThat(stats.getValueCount(), equalTo((long) numDocs));
        assertThat(offHeapSize(stats), greaterThan(0L));
    }

    private static long offHeapSize(DenseVectorStats stats) {
        assertEquals(Set.of("dv"), stats.offHeapStats().keySet());
        return stats.offHeapStats().get("dv").values().stream().mapToLong(Long::longValue).sum();
    }

    /**
     * Indexes {@code numDocs} documents, each carrying a single dense vector, into an engine configured with the given
     * node settings, and returns the stats it reports for them.
     */
    private DenseVectorStats denseVectorStats(Settings nodeSettings, int numDocs) throws IOException {
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(defaultSettings.getIndex(), indexSettings(), nodeSettings);
        assertEquals(nodeSettings.isEmpty() == false, DiscoveryNode.isStateless(indexSettings.getNodeSettings()));

        try (Store store = createStore()) {
            final EngineConfig config = config(indexSettings, store, createTempDir(), NoMergePolicy.INSTANCE);
            final DocumentMapper documentMapper = config.getMapperService().documentMapper();
            try (InternalEngine engine = createEngine(config)) {
                indexDenseVectorDocs(engine, documentMapper, numDocs);
                engine.refresh("test");
                return engine.denseVectorStats(config.getMapperService().mappingLookup());
            }
        }
    }

    /**
     * Indexes {@code numDocs} documents into the given engine, each with a single, distinct value for the {@code dv} field,
     * so that the resulting vector count is known exactly.
     */
    private void indexDenseVectorDocs(InternalEngine engine, DocumentMapper documentMapper, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            final String source = Strings.format("{\"dv\":[%s,%s,%s]}", randomFloat() + 1, randomFloat() + 1, randomFloat() + 1);
            engine.index(indexForDoc(documentMapper.parse(new SourceToParse("d_" + i, new BytesArray(source), XContentType.JSON))));
        }
    }
}
