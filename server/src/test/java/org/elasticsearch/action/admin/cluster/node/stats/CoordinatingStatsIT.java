package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.metrics.MetricsConstant;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

/**
 * @author kyra.wkh
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CoordinatingStatsIT extends ESIntegTestCase {

    public void testCoordinatingMetric() throws Exception {
        internalCluster().startMasterOnlyNodes(1);
        String dataNode = internalCluster().startDataOnlyNode();
        String coordinatingNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        String index1 = "index1";
        createIndex(index1);
        ensureGreen(index1);

        String index2 = "index2";
        createIndex(index2);
        ensureGreen(index2);

        client(coordinatingNode).prepareSearch(index1).get();
        client(coordinatingNode).prepareSearch(index2).get();
        CoordinatingStats coordinatingStats = getNodeCoordinatingStats(coordinatingNode);
        // check total qps
        CoordinatingIndiceStats totalCoordinatingNode = coordinatingStats.getTotal();
        assertEquals(2, totalCoordinatingNode.counterMetricMap.get(MetricsConstant.SEARCH_TOTAL).count());
        // check indices qps
        assertEquals(2, coordinatingStats.getIndiceStats().size());
        for (CoordinatingIndiceStats indiceStats : coordinatingStats.getIndiceStats()) {
            assertEquals(1, indiceStats.counterMetricMap.get(MetricsConstant.SEARCH_TOTAL).count());
        }

        // check total latency
        assertTrue(totalCoordinatingNode.meanMatricMap.get(MetricsConstant.SEARCH_LATENCY_MILLIS).sum() > 0);
        // check index latency
        for (CoordinatingIndiceStats indiceStats : getNodeCoordinatingStats(coordinatingNode).getIndiceStats()) {
            assertTrue(indiceStats.meanMatricMap.get(MetricsConstant.SEARCH_LATENCY_MILLIS).sum() > 0);
        }

        // check data node cannot record
        CoordinatingIndiceStats totalDataNode = getNodeCoordinatingStats(dataNode).getTotal();
        assertEquals(0, totalDataNode.counterMetricMap.get(MetricsConstant.SEARCH_TOTAL).count());
        assertEquals(0, totalDataNode.meanMatricMap.get(MetricsConstant.SEARCH_LATENCY_MILLIS).sum());

        // restart coordinating only node
        internalCluster().restartNode(coordinatingNode, InternalTestCluster.EMPTY_CALLBACK);
        Thread.sleep(5000);
        totalCoordinatingNode = getNodeCoordinatingStats(coordinatingNode).getTotal();
        // check restart clears metric
        assertEquals(0, totalCoordinatingNode.counterMetricMap.get(MetricsConstant.SEARCH_TOTAL).count());
        client(coordinatingNode).prepareSearch(index1).get();
        client(coordinatingNode).prepareSearch(index2).get();
        // check total
        totalCoordinatingNode = getNodeCoordinatingStats(coordinatingNode).getTotal();
        assertEquals(2, totalCoordinatingNode.counterMetricMap.get(MetricsConstant.SEARCH_TOTAL).count());

        // check total qps
        totalCoordinatingNode = getNodeCoordinatingStats(coordinatingNode).getTotal();
        assertEquals(2, totalCoordinatingNode.counterMetricMap.get(MetricsConstant.SEARCH_TOTAL).count());
        // check indices qps
        assertEquals(2, getNodeCoordinatingStats(coordinatingNode).getIndiceStats().size());
        for (CoordinatingIndiceStats indiceStats : getNodeCoordinatingStats(coordinatingNode).getIndiceStats()) {
            assertEquals(1, indiceStats.counterMetricMap.get(MetricsConstant.SEARCH_TOTAL).count());
        }

        // check total latency
        assertTrue(totalCoordinatingNode.meanMatricMap.get(MetricsConstant.SEARCH_LATENCY_MILLIS).sum() > 0);
        // check index latency
        for (CoordinatingIndiceStats indiceStats : getNodeCoordinatingStats(coordinatingNode).getIndiceStats()) {
            assertTrue(indiceStats.meanMatricMap.get(MetricsConstant.SEARCH_LATENCY_MILLIS).sum() > 0);
        }

        // check delete index removes related metric
        ElasticsearchAssertions.assertAcked(client().admin().indices().prepareDelete(index1).get());
        assertEquals(1, getNodeCoordinatingStats(coordinatingNode).getIndiceStats().size());
        assertEquals(index2, getNodeCoordinatingStats(coordinatingNode).getIndiceStats().get(0).indexName);

        logger.info("start new coordinating node");
        // start another coordinating ndoe
        String coordinatingNode2 = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        client(coordinatingNode2).prepareSearch(index2).get();
        // check total qps
        CoordinatingIndiceStats totalCoordinatingNode2 = getNodeCoordinatingStats(coordinatingNode2).getTotal();
        assertEquals(1, totalCoordinatingNode2.counterMetricMap.get(MetricsConstant.SEARCH_TOTAL).count());
    }


    private void indexDoc(String index, String contentJson) {
        IndexResponse response = client().prepareIndex(index)
            .setSource(contentJson, XContentType.JSON)
            .get();
        if (!response.status().equals(RestStatus.OK) && !response.status().equals(RestStatus.CREATED)) {
            fail("Bad response while adding doc");
        }
    }


    private CoordinatingStats getNodeCoordinatingStats(String nodeName) {
        return internalCluster().getInstance(NodeService.class, nodeName)
            .stats(CommonStatsFlags.ALL, true, true, true, true, true, true, true, true, true, true, true, true, true)
            .getCoordinatingStats();
    }
}
