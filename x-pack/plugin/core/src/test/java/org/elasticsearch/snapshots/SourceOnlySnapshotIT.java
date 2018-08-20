package org.elasticsearch.snapshots;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.engine.MockEngineFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class SourceOnlySnapshotIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> classes = new ArrayList<>(super.nodePlugins());
        classes.add(MyPlugin.class);
        return classes;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Collection<Class<? extends Plugin>> mockPlugins = super.getMockPlugins();
        Collection<Class<? extends Plugin>> classes = new ArrayList<>(super.getMockPlugins());
        classes.remove(MockEngineFactoryPlugin.class);
        return classes;
    }

    public static final class MyPlugin extends Plugin implements RepositoryPlugin, EnginePlugin {
        @Override
        public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry) {
            return Collections.singletonMap("source", SourceOnlySnapshotRepository.newFactory());
        }
        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (indexSettings.getValue(SourceOnlySnapshotRepository.SOURCE_ONLY_ENGINE)) {
                EngineFactory engineFactory = SourceOnlySnapshotEngine::new;
                return Optional.of(engineFactory);
            }
            return Optional.empty();
        }

        @Override
        public List<Setting<?>> getSettings() {
            List<Setting<?>> settings = new ArrayList<>(super.getSettings());
            settings.add(SourceOnlySnapshotRepository.SOURCE_ONLY_ENGINE);
            return settings;
        }

    }

    /**
     * Tests that a source only index snapshot
     */
    public void testSnapshotAndRestore() throws Exception {
        final String sourceIdx = "test-idx";
        IndexRequestBuilder[] builders = snashotAndRestore(sourceIdx, 1, false, false);

        SearchResponse searchResponse = client().prepareSearch(sourceIdx).setSize(builders.length)
            .addSort(SeqNoFieldMapper.NAME, SortOrder.ASC).get();
        SearchHits hits = searchResponse.getHits();
        assertEquals(builders.length, hits.totalHits);
        long i = 0;
        for (SearchHit hit : hits) {
            String id = hit.getId();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            assertTrue(sourceAsMap.containsKey("field1"));
            assertEquals(i++, hit.getSortValues()[0]);
            assertEquals("bar "+id, sourceAsMap.get("field1"));
            assertEquals("r"+id, hit.field("_routing").getValue());
        }
        // ensure we can find hits
        assertHitCount(client().prepareSearch(sourceIdx).setQuery(QueryBuilders.idsQuery()
            .addIds("" + randomIntBetween(0, builders.length))).get(), 1);
        assertHitCount(client().prepareSearch(sourceIdx).setQuery(QueryBuilders.termQuery("field1", "bar")).get(), builders.length);
    }

    /**
     * Tests that a source only index snapshot
     */
    public void testSnapshotAndRestoreMinimal() throws Exception {
        final String sourceIdx = "test-idx";
        try {
            boolean requireRouting = randomBoolean();
            IndexRequestBuilder[] builders = snashotAndRestore(sourceIdx, 1, true, requireRouting);

            SearchResponse searchResponse = client().prepareSearch(sourceIdx)
                .addSort(SeqNoFieldMapper.NAME, SortOrder.ASC)
                .setSize(builders.length).get();
            SearchHits hits = searchResponse.getHits();
            assertEquals(builders.length, hits.totalHits);
            long i = 0;
            for (SearchHit hit : hits) {
                String id = hit.getId();
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                assertTrue(sourceAsMap.containsKey("field1"));
                assertEquals(i++, hit.getSortValues()[0]);
                assertEquals("bar " + id, sourceAsMap.get("field1"));
                assertEquals("r" + id, hit.field("_routing").getValue());
            }
            GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(sourceIdx).get();
            ImmutableOpenMap<String, MappingMetaData> mapping = getMappingsResponse
                .getMappings().get(sourceIdx);
            assertTrue(mapping.containsKey("_doc"));
            if (requireRouting) {
                assertEquals("{\"_doc\":{\"enabled\":false," +
                    "\"_meta\":{\"_doc\":{\"_routing\":{\"required\":true}," +
                    "\"properties\":{\"field1\":{\"type\":\"text\"," +
                    "\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}," +
                    "\"_routing\":{\"required\":true}}}", mapping.get("_doc").source().string());
            } else {
                assertEquals("{\"_doc\":{\"enabled\":false," +
                    "\"_meta\":{\"_doc\":{\"properties\":{\"field1\":{\"type\":\"text\"," +
                    "\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}}}", mapping.get("_doc").source().string());
            }
//        assertHitCount(client().prepareSearch(sourceIdx).setQuery(QueryBuilders.idsQuery()
//            .addIds("" + randomIntBetween(0, builders.length))).get(), 1);
//        // ensure we can not find hits it's a minimal restore
//        assertHitCount(client().prepareSearch(sourceIdx).setQuery(QueryBuilders.termQuery("field1", "bar")).get(), 0);
//        // make sure deletes work
//        String idToDelete = "" + randomIntBetween(0, builders.length);
//        DeleteResponse deleteResponse = client().prepareDelete(sourceIdx, "_doc", idToDelete).setRouting("r" + idToDelete).get();
//        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
//        refresh(sourceIdx);
//        assertHitCount(client().prepareSearch(sourceIdx).setQuery(QueryBuilders.idsQuery().addIds(idToDelete)).get(), 0);
            internalCluster().ensureAtLeastNumDataNodes(2);
            client().admin().indices().prepareUpdateSettings(sourceIdx).setSettings(Settings.builder().put("index.number_of_replicas", 1))
                .get();
            ensureGreen(sourceIdx);
        } finally {
            client().admin().indices().prepareDelete(sourceIdx).get();
        }

    }

    private IndexRequestBuilder[] snashotAndRestore(String sourceIdx, int numShards, boolean minimal, boolean requireRouting)
        throws ExecutionException, InterruptedException, IOException {
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
        if (requireRouting) {
            createIndexRequestBuilder.addMapping("_doc", "_routing", "required=true");
        }
        assertAcked(createIndexRequestBuilder);
        ensureGreen();

        logger.info("--> indexing some data");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[randomIntBetween(10, 100)];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(sourceIdx, "_doc",
                Integer.toString(i)).setSource("field1", "bar " + i).setRouting("r" + i);
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

        logger.info("--> start a new data node");
        final Settings dataSettings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLength(5))
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()) // to get a new node id
            .build();
        internalCluster().startDataOnlyNode(dataSettings);
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
