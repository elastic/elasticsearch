package org.elasticsearch.percolator;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.index.mapper.DocumentTypeListener;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.AbstractNodesTests;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class DeletePercolatorTypeTests extends AbstractNodesTests {

    public void beforeClass() throws Exception {
        startNode("node1");
        startNode("node2");
    }

    @Test
    public void testDeletePercolatorType() throws Exception {
        DeleteIndexResponse deleteIndexResponse = client().admin().indices().prepareDelete().execute().actionGet();
        assertThat("Delete Index failed - not acked", deleteIndexResponse.isAcknowledged(), equalTo(true));
        ensureGreen();

        client().admin().indices().prepareCreate("test1").execute().actionGet();
        client().admin().indices().prepareCreate("test2").execute().actionGet();
        ensureGreen();

        client().prepareIndex("test1", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().prepareIndex("test2", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();

        PercolateResponse response = client().preparePercolate()
                .setIndices("test1", "test2").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertThat(response.getCount(), equalTo(2l));

        CountDownLatch test1Latch = createCountDownLatch("test1");
        CountDownLatch test2Latch =createCountDownLatch("test2");

        client().admin().indices().prepareDeleteMapping("test1").setType("_percolator").execute().actionGet();
        test1Latch.await();

        response = client().preparePercolate()
                .setIndices("test1", "test2").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(1l));

        client().admin().indices().prepareDeleteMapping("test2").setType("_percolator").execute().actionGet();
        test2Latch.await();

        // Percolate api should return 0 matches, because all _percolate types have been removed.
        response = client().preparePercolate()
                .setIndices("test1", "test2").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(0l));
    }

    private CountDownLatch createCountDownLatch(String index) {
        List<Node> nodes = nodes();
        final CountDownLatch latch = new CountDownLatch(nodes.size());
        for (Node node : nodes) {
            IndicesService indexServices = ((InternalNode) node).injector().getInstance(IndicesService.class);
            MapperService mapperService = indexServices.indexService(index).mapperService();
            mapperService.addTypeListener(new DocumentTypeListener() {
                @Override
                public void created(String type) {
                }

                @Override
                public void removed(String type) {
                    latch.countDown();
                }
            });
        }
        return latch;
    }

    public ClusterHealthStatus ensureGreen() {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        assertThat(actionGet.isTimedOut(), equalTo(false));
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        return actionGet.getStatus();
    }

}
