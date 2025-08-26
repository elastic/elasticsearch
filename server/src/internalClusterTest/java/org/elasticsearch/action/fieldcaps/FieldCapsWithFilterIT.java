/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class FieldCapsWithFilterIT extends ESIntegTestCase {
    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private static class EngineWithExposingTimestamp extends InternalEngine {
        EngineWithExposingTimestamp(EngineConfig engineConfig) {
            super(engineConfig);
            assert IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(config().getIndexSettings().getSettings()) : "require read-only index";
        }

        @Override
        public ShardLongFieldRange getRawFieldRange(String field) {
            try (Searcher searcher = acquireSearcher("test")) {
                final DirectoryReader directoryReader = searcher.getDirectoryReader();

                final byte[] minPackedValue = PointValues.getMinPackedValue(directoryReader, field);
                final byte[] maxPackedValue = PointValues.getMaxPackedValue(directoryReader, field);
                if (minPackedValue == null || maxPackedValue == null) {
                    assert minPackedValue == null && maxPackedValue == null
                        : Arrays.toString(minPackedValue) + "-" + Arrays.toString(maxPackedValue);
                    return ShardLongFieldRange.EMPTY;
                }

                return ShardLongFieldRange.of(LongPoint.decodeDimension(minPackedValue, 0), LongPoint.decodeDimension(maxPackedValue, 0));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static class ExposingTimestampEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(EngineWithExposingTimestamp::new);
            } else {
                return Optional.of(new InternalEngineFactory());
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ExposingTimestampEnginePlugin.class);
    }

    void createIndexAndIndexDocs(String index, Settings.Builder indexSettings, long timestamp, boolean exposeTimestamp) throws Exception {
        Client client = client();
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setSettings(indexSettings)
                .setMapping("@timestamp", "type=date", "position", "type=long")
        );
        int numDocs = between(100, 500);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("position", i, "@timestamp", timestamp + i).get();
        }
        if (exposeTimestamp) {
            client.admin().indices().prepareClose(index).get();
            client.admin()
                .indices()
                .prepareUpdateSettings(index)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
                .get();
            client.admin().indices().prepareOpen(index).get();
            assertBusy(() -> {
                IndexLongFieldRange timestampRange = clusterService().state().metadata().index(index).getTimestampRange();
                assertTrue(Strings.toString(timestampRange), timestampRange.containsAllShardRanges());
            });
        } else {
            client.admin().indices().prepareRefresh(index).get();
        }
    }

    public void testSkipUnmatchedShards() throws Exception {
        long oldTimestamp = randomLongBetween(10_000_000, 20_000_000);
        long newTimestamp = randomLongBetween(30_000_000, 50_000_000);
        String redNode = internalCluster().startDataOnlyNode();
        String blueNode = internalCluster().startDataOnlyNode();
        createIndexAndIndexDocs(
            "index_old",
            indexSettings(between(1, 5), 0).put("index.routing.allocation.include._name", redNode),
            oldTimestamp,
            true
        );
        internalCluster().stopNode(redNode);
        createIndexAndIndexDocs(
            "index_new",
            indexSettings(between(1, 5), 0).put("index.routing.allocation.include._name", blueNode),
            newTimestamp,
            false
        );
        // fails without index filter
        {
            FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
            request.indices("index_*");
            request.fields("*");
            request.setMergeResults(false);
            if (randomBoolean()) {
                request.indexFilter(new RangeQueryBuilder("@timestamp").from(oldTimestamp));
            }
            var response = safeGet(client().execute(TransportFieldCapabilitiesAction.TYPE, request));
            assertThat(response.getIndexResponses(), hasSize(1));
            assertThat(response.getIndexResponses().get(0).getIndexName(), equalTo("index_new"));
            assertThat(response.getFailures(), hasSize(1));
            assertThat(response.getFailures().get(0).getIndices(), equalTo(new String[] { "index_old" }));
            assertThat(response.getFailures().get(0).getException(), instanceOf(NoShardAvailableActionException.class));
        }
        // skip unavailable shards with index filter
        {
            FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
            request.indices("index_*");
            request.fields("*");
            request.indexFilter(new RangeQueryBuilder("@timestamp").from(newTimestamp));
            request.setMergeResults(false);
            var response = safeGet(client().execute(TransportFieldCapabilitiesAction.TYPE, request));
            assertThat(response.getIndexResponses(), hasSize(1));
            assertThat(response.getIndexResponses().get(0).getIndexName(), equalTo("index_new"));
            assertThat(response.getFailures(), empty());
        }
        // skip both indices on the coordinator, one the data nodes
        {
            FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
            request.indices("index_*");
            request.fields("*");
            request.indexFilter(new RangeQueryBuilder("@timestamp").from(newTimestamp * 2L));
            request.setMergeResults(false);
            var response = safeGet(client().execute(TransportFieldCapabilitiesAction.TYPE, request));
            assertThat(response.getIndexResponses(), empty());
            assertThat(response.getFailures(), empty());
        }
    }
}
