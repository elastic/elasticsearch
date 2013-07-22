package org.elasticsearch.test.integration.percolator;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.integration.percolator.SimplePercolatorTests.convertFromTextArray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
public class TTLPercolatorTests extends AbstractNodesTests {

    @Test
    public void testPercolatingWithTimeToLive() throws Exception {
        long purgeInterval = 200;
        Settings settings = settingsBuilder()
                .put("gateway.type", "none")
                .put("indices.ttl.interval", purgeInterval).build(); // <-- For testing ttl.
        logger.info("--> starting 2 nodes");
        startNode("node1", settings);
        startNode("node2", settings);

        Client client = client("node1");
        client.admin().indices().prepareDelete().execute().actionGet();
        ensureGreen(client);

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("_percolator")
                .startObject("_ttl").field("enabled", true).endObject()
                .startObject("_timestamp").field("enabled", true).endObject()
                .endObject().endObject().string();

        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("_percolator", mapping)
                .addMapping("type1", mapping)
                .execute().actionGet();
        ensureGreen(client);

        long ttl = 1500;
        long now = System.currentTimeMillis();
        client.prepareIndex("test", "_percolator", "kuku").setSource(jsonBuilder()
                .startObject()
                .startObject("query")
                .startObject("term")
                .field("field1", "value1")
                .endObject()
                .endObject()
                .endObject()
        ).setRefresh(true).setTTL(ttl).execute().actionGet();

        PercolateResponse percolateResponse = client.preparePercolate("test", "type1").setSource(jsonBuilder()
                .startObject()
                .startObject("doc")
                .field("field1", "value1")
                .endObject()
                .endObject()
        ).execute().actionGet();
        assertThat(convertFromTextArray(percolateResponse.getMatches()), arrayContaining("kuku"));

        long timeSpent = System.currentTimeMillis() - now;
        long waitTime = ttl + purgeInterval + 200;
        if (timeSpent <= waitTime) {
            long timeToWait = waitTime - timeSpent;
            logger.info("Waiting {} ms for ttl purging...", timeToWait);
            Thread.sleep(timeToWait);
        }
        percolateResponse = client.preparePercolate("test", "type1").setSource(jsonBuilder()
                .startObject()
                .startObject("doc")
                .field("field1", "value1")
                .endObject()
                .endObject()
        ).execute().actionGet();
        assertThat(percolateResponse.getMatches(), emptyArray());
    }

    @AfterMethod
    public void cleanAndCloseNodes() throws Exception {
        closeAllNodes();
    }

    public static void ensureGreen(Client client) {
        ClusterHealthResponse actionGet = client.admin().cluster()
                .health(Requests.clusterHealthRequest().waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        assertThat(actionGet.isTimedOut(), equalTo(false));
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }

}
