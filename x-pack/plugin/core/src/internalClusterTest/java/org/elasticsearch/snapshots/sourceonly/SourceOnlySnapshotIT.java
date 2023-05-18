/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.snapshots.sourceonly;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class SourceOnlySnapshotIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MyPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    public void setUp() throws Exception {
        disableRepoConsistencyCheck("source only snapshot repository does not support consistency check");
        super.setUp();
    }

    public static final class MyPlugin extends Plugin implements RepositoryPlugin, EnginePlugin {
        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap("source", SourceOnlySnapshotRepository.newRepositoryFactory());
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (indexSettings.getValue(SourceOnlySnapshotRepository.SOURCE_ONLY)) {
                return Optional.of(SourceOnlySnapshotRepository.getEngineFactory());
            }
            return Optional.empty();
        }

        @Override
        public List<Setting<?>> getSettings() {
            return CollectionUtils.appendToCopy(super.getSettings(), SourceOnlySnapshotRepository.SOURCE_ONLY);
        }
    }

    public void testSnapshotAndRestore() throws Exception {
        final String sourceIdx = "test-idx";
        boolean requireRouting = randomBoolean();
        boolean useNested = randomBoolean();
        IndexRequestBuilder[] builders = snapshotAndRestore(sourceIdx, requireRouting, useNested);
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(sourceIdx).clear().setDocs(true).get();
        long deleted = indicesStatsResponse.getTotal().docs.getDeleted();
        boolean sourceHadDeletions = deleted > 0; // we use indexRandom which might create holes ie. deleted docs
        assertHits(sourceIdx, builders.length, sourceHadDeletions);
        assertMappings(sourceIdx, requireRouting, useNested);
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> {
            client().prepareSearch(sourceIdx).setQuery(QueryBuilders.idsQuery().addIds("" + randomIntBetween(0, builders.length))).get();
        });
        assertTrue(e.toString().contains("_source only indices can't be searched or filtered"));

        // can-match phase pre-filters access to non-existing field
        assertEquals(
            0,
            client().prepareSearch(sourceIdx).setQuery(QueryBuilders.termQuery("field1", "bar")).get().getHits().getTotalHits().value
        );
        // make sure deletes do not work
        String idToDelete = "" + randomIntBetween(0, builders.length);
        expectThrows(ClusterBlockException.class, () -> client().prepareDelete(sourceIdx, idToDelete).setRouting("r" + idToDelete).get());
        internalCluster().ensureAtLeastNumDataNodes(2);
        setReplicaCount(1, sourceIdx);
        ensureGreen(sourceIdx);
        assertHits(sourceIdx, builders.length, sourceHadDeletions);
    }

    public void testSnapshotAndRestoreWithNested() throws Exception {
        final String sourceIdx = "test-idx";
        boolean requireRouting = randomBoolean();
        IndexRequestBuilder[] builders = snapshotAndRestore(sourceIdx, requireRouting, true);
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats().clear().setDocs(true).get();
        assertThat(indicesStatsResponse.getTotal().docs.getDeleted(), Matchers.greaterThan(0L));
        assertHits(sourceIdx, builders.length, true);
        assertMappings(sourceIdx, requireRouting, true);
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch(sourceIdx)
                .setQuery(QueryBuilders.idsQuery().addIds("" + randomIntBetween(0, builders.length)))
                .get()
        );
        assertTrue(e.toString().contains("_source only indices can't be searched or filtered"));
        // can-match phase pre-filters access to non-existing field
        assertEquals(
            0,
            client().prepareSearch(sourceIdx).setQuery(QueryBuilders.termQuery("field1", "bar")).get().getHits().getTotalHits().value
        );
        // make sure deletes do not work
        String idToDelete = "" + randomIntBetween(0, builders.length);
        expectThrows(ClusterBlockException.class, () -> client().prepareDelete(sourceIdx, idToDelete).setRouting("r" + idToDelete).get());
        internalCluster().ensureAtLeastNumDataNodes(2);
        setReplicaCount(1, sourceIdx);
        ensureGreen(sourceIdx);
        assertHits(sourceIdx, builders.length, true);
    }

    public void testSnapshotWithDanglingLocalSegment() throws Exception {
        logger.info("-->  starting a master node and a data node");
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String repo = "test-repo";
        createRepository(
            repo,
            "source",
            Settings.builder().put("location", randomRepoPath()).put("delegate_type", "fs").put("compress", randomBoolean())
        );

        final String indexName = "test-idx";
        createIndex(indexName);
        client().prepareIndex(indexName).setSource("foo", "bar").get();
        assertSuccessful(startFullSnapshot(repo, "snapshot-1"));

        client().prepareIndex(indexName).setSource("foo", "baz").get();
        assertSuccessful(startFullSnapshot(repo, "snapshot-2"));

        logger.info("--> randomly deleting files from the local _snapshot path to simulate corruption");
        Path snapshotShardPath = internalCluster().getInstance(IndicesService.class, dataNode)
            .indexService(clusterService().state().metadata().index(indexName).getIndex())
            .getShard(0)
            .shardPath()
            .getDataPath()
            .resolve("_snapshot");
        try (DirectoryStream<Path> localFiles = Files.newDirectoryStream(snapshotShardPath)) {
            for (Path localFile : localFiles) {
                if (randomBoolean()) {
                    Files.delete(localFile);
                }
            }
        }

        assertSuccessful(startFullSnapshot(repo, "snapshot-3"));
    }

    private static void assertMappings(String sourceIdx, boolean requireRouting, boolean useNested) throws IOException {
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(sourceIdx).get();
        MappingMetadata mapping = getMappingsResponse.getMappings().get(sourceIdx);
        String nested = useNested ? """
            ,"incorrect":{"type":"object"},"nested":{"type":"nested","properties":{"value":{"type":"long"}}}""" : "";
        if (requireRouting) {
            assertEquals(XContentHelper.stripWhitespace(String.format(java.util.Locale.ROOT, """
                {
                  "_doc": {
                    "enabled": false,
                    "_meta": {
                      "_doc": {
                        "_routing": {
                          "required": true
                        },
                        "properties": {
                          "field1": {
                            "type": "text",
                            "fields": {
                              "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                              }
                            }
                          }
                          %s
                        }
                      }
                    }
                  }
                }""", nested)), mapping.source().string());
        } else {
            assertEquals(XContentHelper.stripWhitespace(String.format(java.util.Locale.ROOT, """
                {
                  "_doc": {
                    "enabled": false,
                    "_meta": {
                      "_doc": {
                        "properties": {
                          "field1": {
                            "type": "text",
                            "fields": {
                              "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                              }
                            }
                          }
                          %s
                        }
                      }
                    }
                  }
                }""", nested)), mapping.source().string());
        }
    }

    private void assertHits(String index, int numDocsExpected, boolean sourceHadDeletions) {
        SearchResponse searchResponse = client().prepareSearch(index)
            .addSort(SeqNoFieldMapper.NAME, SortOrder.ASC)
            .setSize(numDocsExpected)
            .get();
        BiConsumer<SearchResponse, Boolean> assertConsumer = (res, allowHoles) -> {
            SearchHits hits = res.getHits();
            long i = 0;
            for (SearchHit hit : hits) {
                String id = hit.getId();
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                assertTrue(sourceAsMap.containsKey("field1"));
                if (allowHoles) {
                    long seqId = ((Number) hit.getSortValues()[0]).longValue();
                    assertThat(i, Matchers.lessThanOrEqualTo(seqId));
                    i = seqId + 1;
                } else {
                    assertEquals(i++, hit.getSortValues()[0]);
                }
                assertEquals("bar " + id, sourceAsMap.get("field1"));
                assertEquals("r" + id, hit.field("_routing").getValue());
            }
        };
        assertConsumer.accept(searchResponse, sourceHadDeletions);
        assertEquals(numDocsExpected, searchResponse.getHits().getTotalHits().value);
        searchResponse = client().prepareSearch(index)
            .addSort(SeqNoFieldMapper.NAME, SortOrder.ASC)
            .setScroll("1m")
            .slice(new SliceBuilder(SeqNoFieldMapper.NAME, randomIntBetween(0, 1), 2))
            .setSize(randomIntBetween(1, 10))
            .get();
        try {
            do {
                // now do a scroll with a slice
                assertConsumer.accept(searchResponse, true);
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).get();
            } while (searchResponse.getHits().getHits().length > 0);
        } finally {
            if (searchResponse.getScrollId() != null) {
                client().prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();
            }
        }
    }

    private IndexRequestBuilder[] snapshotAndRestore(final String sourceIdx, final boolean requireRouting, final boolean useNested)
        throws InterruptedException, IOException {
        logger.info("-->  starting a master node and a data node");
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final String repo = "test-repo";
        final String snapshot = "test-snap";

        createRepository(
            repo,
            "source",
            Settings.builder().put("location", randomRepoPath()).put("delegate_type", "fs").put("compress", randomBoolean())
        );

        CreateIndexRequestBuilder createIndexRequestBuilder = prepareCreate(sourceIdx, 0, indexSettingsNoReplicas(1));
        List<String> mappings = new ArrayList<>();
        if (requireRouting) {
            mappings.addAll(Arrays.asList("_routing", "required=true"));
        }

        if (useNested) {
            mappings.addAll(Arrays.asList("nested", "type=nested", "incorrect", "type=object"));
        }
        if (mappings.isEmpty() == false) {
            createIndexRequestBuilder.setMapping(mappings.toArray(new String[0]));
        }
        assertAcked(createIndexRequestBuilder);
        ensureGreen();

        logger.info("--> indexing some data");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[randomIntBetween(10, 100)];
        for (int i = 0; i < builders.length; i++) {
            XContentBuilder source = jsonBuilder().startObject().field("field1", "bar " + i);
            if (useNested) {
                source.startArray("nested");
                for (int j = 0; j < 2; ++j) {
                    source = source.startObject().field("value", i + 1 + j).endObject();
                }
                source.endArray();
            }
            source.endObject();
            builders[i] = client().prepareIndex(sourceIdx).setId(Integer.toString(i)).setSource(source).setRouting("r" + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();
        assertHitCount(client().prepareSearch(sourceIdx).setQuery(QueryBuilders.idsQuery().addIds("0")).get(), 1);

        createSnapshot(repo, snapshot, Collections.singletonList(sourceIdx));

        logger.info("--> delete index and stop the data node");
        assertAcked(client().admin().indices().prepareDelete(sourceIdx).get());
        internalCluster().stopRandomDataNode();
        assertFalse(client().admin().cluster().prepareHealth().setTimeout("30s").setWaitForNodes("1").get().isTimedOut());

        final String newDataNode = internalCluster().startDataOnlyNode();
        logger.info("--> start a new data node " + newDataNode);
        assertFalse(client().admin().cluster().prepareHealth().setTimeout("30s").setWaitForNodes("2").get().isTimedOut());

        logger.info("--> restore the index and ensure all shards are allocated");
        RestoreSnapshotResponse restoreResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(repo, snapshot)
            .setWaitForCompletion(true)
            .setIndices(sourceIdx)
            .get();
        assertEquals(restoreResponse.getRestoreInfo().totalShards(), restoreResponse.getRestoreInfo().successfulShards());
        ensureYellow();
        return builders;
    }
}
