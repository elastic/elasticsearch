package org.elasticsearch.versioning;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class ConcurrentDocumentOperationTests extends ElasticsearchIntegrationTest {

    @Test
    public void concurrentOperationOnSameDocTest() throws Exception {

        logger.info("--> create an index with 1 shard and max replicas based on nodes");
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", cluster().size()-1))
                .execute().actionGet();

        logger.info("execute concurrent updates on the same doc");
        int numberOfUpdates = 100;
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(numberOfUpdates);
        for (int i = 0; i < numberOfUpdates; i++) {
            client().prepareIndex("test", "type1", "1").setSource("field1", i).execute(new ActionListener<IndexResponse>() {
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

        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("done indexing, check all have the same field value");
        Map masterSource = client().prepareGet("test", "type1", "1").execute().actionGet().getSourceAsMap();
        for (int i = 0; i < (cluster().size() * 5); i++) {
            assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().getSourceAsMap(), equalTo(masterSource));
        }
    }
}
