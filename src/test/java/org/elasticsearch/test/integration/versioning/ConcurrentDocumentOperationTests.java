package org.elasticsearch.test.integration.versioning;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
@Test
public class ConcurrentDocumentOperationTests extends AbstractNodesTests {

    @AfterMethod
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void concurrentOperationOnSameDocTest() throws Exception {
        // start 5 nodes
        Node[] nodes = new Node[5];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = startNode(Integer.toString(i));
        }

        logger.info("--> create an index with 1 shard and max replicas based on nodes");
        nodes[0].client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", nodes.length - 1))
                .execute().actionGet();

        logger.info("execute concurrent updates on the same doc");
        int numberOfUpdates = 100;
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(numberOfUpdates);
        for (int i = 0; i < numberOfUpdates; i++) {
            nodes[0].client().prepareIndex("test", "type1", "1").setSource("field1", i).execute(new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse response) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable e) {
                    e.printStackTrace();
                    failure.set(e);
                    latch.countDown();
                }
            });
        }

        latch.await();

        assertThat(failure.get(), nullValue());

        nodes[0].client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("done indexing, check all have the same field value");
        Map masterSource = nodes[0].client().prepareGet("test", "type1", "1").execute().actionGet().getSourceAsMap();
        for (int i = 0; i < (nodes.length * 5); i++) {
            assertThat(nodes[0].client().prepareGet("test", "type1", "1").execute().actionGet().getSourceAsMap(), equalTo(masterSource));
        }
    }
}
