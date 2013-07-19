package org.elasticsearch.test.integration.indices.mapping;

import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class UpdateMappingTests extends AbstractSharedClusterTest {

    @Test
    public void dynamicUpdates() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        long recCount = 20;
        for (int rec = 0; rec < recCount; rec++) {
            client().prepareIndex("test", "type", "rec" + rec).setSource("field" + rec, "some_value").execute().actionGet();
        }
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
        logger.info("Searching");
        CountResponse response = client().prepareCount("test").execute().actionGet();
        assertThat(response.getCount(), equalTo(recCount));
    }

}
