package org.elasticsearch.indices;


import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import org.elasticsearch.test.ESSingleNodeTestCase;

import java.nio.file.Path;
import java.util.*;


import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;


public class IndicesShardPathSingleNodeTests extends ESSingleNodeTestCase {


    public NodeEnvironment getNodeEnvironment() {
        return getInstanceFromNode(NodeEnvironment.class);
    }

    public void testSelectNewPathForMultiIndexShardEvenly() throws Throwable {

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen();
        NodeEnvironment env = getNodeEnvironment();
        Index idx = resolveIndex("test");
        Path[] idxPath = env.indexPaths(idx);
        logger.info("IndexPaths.Length:" + idxPath.length);
        assertEquals(2, idxPath.length);
        logger.info("==>IndexPaths:" + idxPath[0].toString() + "," + idxPath[1].toString() );

        Map<NodeEnvironment.NodePath, Long> nodePathLongMap = env.shardCountPerPath(idx);

        for(Map.Entry<NodeEnvironment.NodePath, Long> entry :nodePathLongMap.entrySet()) {
            logger.info("==>Index:{}, shardCountPerPath Path {}, Count {}", "test", entry.getKey().path, entry.getValue());
        }

        assertAcked(client().admin().indices().prepareCreate("test2")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen();

        Index idx2 = resolveIndex("test2");
        Map<NodeEnvironment.NodePath, Long> nodePathLongMap2 = env.shardCountPerPath(idx2);
        for(Map.Entry<NodeEnvironment.NodePath, Long> entry :nodePathLongMap2.entrySet()) {
            logger.info("==>Index:{}, shardCountPerPath Path {}, Count {}", "test2", entry.getKey().path, entry.getValue());
        }

        nodePathLongMap2.forEach(
            (k, v) ->nodePathLongMap.merge(k, v, (v1, v2) -> Long.valueOf(v1 + v2))
        );
        for(Map.Entry<NodeEnvironment.NodePath, Long> entry :nodePathLongMap.entrySet()) {
            logger.info("==>Merge: shardCountPerPath Path {}, Count {}", entry.getKey().path, entry.getValue());
            //Every Path Only have 1 shard
            assertEquals(1, entry.getValue().intValue());
        }

    }

    @Override
    protected Settings nodeSettings() {
        final Path tempDir = createTempDir();
        String[] paths = new String[] {tempDir.resolve("a").toString(),
            tempDir.resolve("b").toString()};
        return Settings.builder()
            .putList(Environment.PATH_DATA_SETTING.getKey(), paths).build();
    }
}
