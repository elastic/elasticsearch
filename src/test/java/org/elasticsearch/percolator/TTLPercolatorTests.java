package org.elasticsearch.percolator;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.percolator.PercolatorTests.convertFromTextArray;
import static org.elasticsearch.test.hamcrest.ElasticSearchAssertions.assertNoFailures;
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

        IndicesStatsResponse response = client.admin().indices().prepareStats("test")
                .clear().setIndexing(true)
                .execute().actionGet();
        assertThat(response.getIndices().get("test").getTotal().getIndexing().getTotal().getIndexCount(), equalTo(2l));

        PercolateResponse percolateResponse = client.preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder()
                .startObject()
                .startObject("doc")
                .field("field1", "value1")
                .endObject()
                .endObject()
        ).execute().actionGet();
        assertNoFailures(percolateResponse);
        if (percolateResponse.getMatches().length == 0) {
            // OK, ttl + purgeInterval has passed (slow machine or many other tests were running at the same time
            GetResponse getResponse = client.prepareGet("test", "_percolator", "kuku").execute().actionGet();
            assertThat(getResponse.isExists(), equalTo(false));
            response = client.admin().indices().prepareStats("test")
                    .clear().setIndexing(true)
                    .execute().actionGet();
            long currentDeleteCount = response.getIndices().get("test").getTotal().getIndexing().getTotal().getDeleteCount();
            assertThat(currentDeleteCount, equalTo(2l));
            return;
        }

        assertThat(convertFromTextArray(percolateResponse.getMatches(), "test"), arrayContaining("kuku"));
        long timeSpent = System.currentTimeMillis() - now;
        long waitTime = ttl + purgeInterval - timeSpent;
        if (waitTime >= 0) {
            Thread.sleep(waitTime); // Doesn't make sense to check the deleteCount before ttl has expired
        }

        // See comment in SimpleTTLTests
        logger.info("Checking if the ttl purger has run");
        long currentDeleteCount;
        do {
            response = client.admin().indices().prepareStats("test")
                    .clear().setIndexing(true)
                    .execute().actionGet();
            // This returns the number of delete operations stats (not Lucene delete count)
            currentDeleteCount = response.getIndices().get("test").getTotal().getIndexing().getTotal().getDeleteCount();
        } while (currentDeleteCount < 2); // TTL deletes one doc, but it is indexed in the primary shard and replica shard.
        assertThat(currentDeleteCount, equalTo(2l));

        percolateResponse = client.preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder()
                .startObject()
                .startObject("doc")
                .field("field1", "value1")
                .endObject()
                .endObject()
        ).execute().actionGet();
        assertNoFailures(percolateResponse);
        assertThat(percolateResponse.getMatches(), emptyArray());
    }

    @After
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
