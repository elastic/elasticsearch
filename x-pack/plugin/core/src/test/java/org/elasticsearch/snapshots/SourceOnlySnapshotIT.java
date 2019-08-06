/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class SourceOnlySnapshotIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> classes = new ArrayList<>(super.nodePlugins());
        classes.add(MyPlugin.class);
        return classes;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public static final class MyPlugin extends Plugin implements RepositoryPlugin, EnginePlugin {
        @Override
        public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                               ThreadPool threadPool) {
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
            List<Setting<?>> settings = new ArrayList<>(super.getSettings());
            settings.add(SourceOnlySnapshotRepository.SOURCE_ONLY);
            return settings;
        }
    }

    public void testSnapshotAndRestore() throws Exception {
        final String sourceIdx = "test-idx";
        boolean requireRouting = randomBoolean();
        boolean useNested = randomBoolean();
        IndexRequestBuilder[] builders = snashotAndRestore(sourceIdx, 1, true, requireRouting, useNested);
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(sourceIdx).clear().setDocs(true).get();
        long deleted = indicesStatsResponse.getTotal().docs.getDeleted();
        boolean sourceHadDeletions = deleted > 0; // we use indexRandom which might create holes ie. deleted docs
        assertHits(sourceIdx, builders.length, sourceHadDeletions);
        assertMappings(sourceIdx, requireRouting, useNested);
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> {
            client().prepareSearch(sourceIdx).setQuery(QueryBuilders.idsQuery()
                .addIds("" + randomIntBetween(0, builders.length))).get();
        });
        assertTrue(e.toString().contains("_source only indices can't be searched or filtered"));

        e = expectThrows(SearchPhaseExecutionException.class, () ->
            client().prepareSearch(sourceIdx).setQuery(QueryBuilders.termQuery("field1", "bar")).get());
        assertTrue(e.toString().contains("_source only indices can't be searched or filtered"));
        // make sure deletes do not work
        String idToDelete = "" + randomIntBetween(0, builders.length);
        expectThrows(ClusterBlockException.class, () -> client().prepareDelete(sourceIdx, "_doc", idToDelete)
            .setRouting("r" + idToDelete).get());
        internalCluster().ensureAtLeastNumDataNodes(2);
            client().admin().indices().prepareUpdateSettings(sourceIdx)
                .setSettings(Settings.builder().put("index.number_of_replicas", 1)).get();
        ensureGreen(sourceIdx);
        assertHits(sourceIdx, builders.length, sourceHadDeletions);
    }

    public void testSnapshotAndRestoreWithNested() throws Exception {
        final String sourceIdx = "test-idx";
        boolean requireRouting = randomBoolean();
        IndexRequestBuilder[] builders = snashotAndRestore(sourceIdx, 1, true, requireRouting, true);
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats().clear().setDocs(true).get();
        assertThat(indicesStatsResponse.getTotal().docs.getDeleted(), Matchers.greaterThan(0L));
        assertHits(sourceIdx, builders.length, true);
        assertMappings(sourceIdx, requireRouting, true);
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () ->
            client().prepareSearch(sourceIdx).setQuery(QueryBuilders.idsQuery().addIds("" + randomIntBetween(0, builders.length))).get());
        assertTrue(e.toString().contains("_source only indices can't be searched or filtered"));
        e = expectThrows(SearchPhaseExecutionException.class, () ->
            client().prepareSearch(sourceIdx).setQuery(QueryBuilders.termQuery("field1", "bar")).get());
        assertTrue(e.toString().contains("_source only indices can't be searched or filtered"));
        // make sure deletes do not work
        String idToDelete = "" + randomIntBetween(0, builders.length);
        expectThrows(ClusterBlockException.class, () -> client().prepareDelete(sourceIdx, "_doc", idToDelete)
            .setRouting("r" + idToDelete).get());
        internalCluster().ensureAtLeastNumDataNodes(2);
        client().admin().indices().prepareUpdateSettings(sourceIdx).setSettings(Settings.builder().put("index.number_of_replicas", 1))
            .get();
        ensureGreen(sourceIdx);
        assertHits(sourceIdx, builders.length, true);
    }

    private void assertMappings(String sourceIdx, boolean requireRouting, boolean useNested) throws IOException {
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(sourceIdx).get();
        ImmutableOpenMap<String, MappingMetaData> mapping = getMappingsResponse
            .getMappings().get(sourceIdx);
        assertTrue(mapping.containsKey("_doc"));
        String nested = useNested ?
            ",\"incorrect\":{\"type\":\"object\"},\"nested\":{\"type\":\"nested\",\"properties\":{\"value\":{\"type\":\"long\"}}}" : "";
        if (requireRouting) {
            assertEquals("{\"_doc\":{\"enabled\":false," +
                "\"_meta\":{\"_doc\":{\"_routing\":{\"required\":true}," +
                "\"properties\":{\"field1\":{\"type\":\"text\"," +
                "\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}" + nested +
                "}}}}}", mapping.get("_doc").source().string());
        } else {
            assertEquals("{\"_doc\":{\"enabled\":false," +
                "\"_meta\":{\"_doc\":{\"properties\":{\"field1\":{\"type\":\"text\"," +
                "\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}" + nested + "}}}}}",
                mapping.get("_doc").source().string());
        }
    }

    private void assertHits(String index, int numDocsExpected, boolean sourceHadDeletions) {
        SearchResponse searchResponse = client().prepareSearch(index)
            .addSort(SeqNoFieldMapper.NAME, SortOrder.ASC)
            .setSize(numDocsExpected).get();
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
            .slice(new SliceBuilder(SeqNoFieldMapper.NAME, randomIntBetween(0,1), 2))
            .setSize(randomIntBetween(1, 10)).get();
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

    private IndexRequestBuilder[] snashotAndRestore(final String sourceIdx,
                                                    final int numShards,
                                                    final boolean minimal,
                                                    final boolean requireRouting,
                                                    final boolean useNested) throws InterruptedException, IOException {
        logger.info("-->  starting a master node and a data node");
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final Client client = client();
        final String repo = "test-repo";
        final String snapshot = "test-snap";

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository(repo).setType("source")
            .setSettings(Settings.builder().put("location", randomRepoPath())
                .put("delegate_type", "fs")
                .put("restore_minimal", minimal)
                .put("compress", randomBoolean())));

        CreateIndexRequestBuilder createIndexRequestBuilder = prepareCreate(sourceIdx, 0, Settings.builder()
            .put("number_of_shards", numShards).put("number_of_replicas", 0));
        List<Object> mappings = new ArrayList<>();
        if (requireRouting) {
            mappings.addAll(Arrays.asList("_routing", "required=true"));
        }

        if (useNested) {
            mappings.addAll(Arrays.asList("nested", "type=nested", "incorrect", "type=object"));
        }
        if (mappings.isEmpty() == false) {
            createIndexRequestBuilder.addMapping("_doc", mappings.toArray());
        }
        assertAcked(createIndexRequestBuilder);
        ensureGreen();

        logger.info("--> indexing some data");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[randomIntBetween(10, 100)];
        for (int i = 0; i < builders.length; i++) {
            XContentBuilder source = jsonBuilder()
                .startObject()
                .field("field1", "bar " + i);
            if (useNested) {
                source.startArray("nested");
                for (int j = 0; j < 2; ++j) {
                    source = source.startObject().field("value", i + 1 + j).endObject();
                }
                source.endArray();
            }
            source.endObject();
            builders[i] = client().prepareIndex(sourceIdx, "_doc",
                Integer.toString(i)).setSource(source).setRouting("r" + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();
        assertHitCount(client().prepareSearch(sourceIdx).setQuery(QueryBuilders.idsQuery().addIds("0")).get(), 1);

        logger.info("--> snapshot the index");
        CreateSnapshotResponse createResponse = client.admin().cluster()
            .prepareCreateSnapshot(repo, snapshot)
            .setWaitForCompletion(true).setIndices(sourceIdx).get();
        assertEquals(SnapshotState.SUCCESS, createResponse.getSnapshotInfo().state());

        logger.info("--> delete index and stop the data node");
        assertAcked(client.admin().indices().prepareDelete(sourceIdx).get());
        internalCluster().stopRandomDataNode();
        client().admin().cluster().prepareHealth().setTimeout("30s").setWaitForNodes("1");

        final String newDataNode = internalCluster().startDataOnlyNode();
        logger.info("--> start a new data node " + newDataNode);
        client().admin().cluster().prepareHealth().setTimeout("30s").setWaitForNodes("2");

        logger.info("--> restore the index and ensure all shards are allocated");
        RestoreSnapshotResponse restoreResponse = client().admin().cluster()
            .prepareRestoreSnapshot(repo, snapshot).setWaitForCompletion(true)
            .setIndices(sourceIdx).get();
        assertEquals(restoreResponse.getRestoreInfo().totalShards(),
            restoreResponse.getRestoreInfo().successfulShards());
        ensureYellow();
        return builders;
    }
}
