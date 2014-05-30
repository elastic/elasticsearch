package org.elasticsearch.script;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class NoShardsSettingTests extends ElasticsearchIntegrationTest {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("gateway.type", "none")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    @Test
    public void testNoOfShards() {
        assertAcked(prepareCreate("test").get());
        assertThat(client().admin().indices().prepareGetSettings("test").get().getSetting("test", "index.number_of_shards"), equalTo("1"));
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    public Settings indexSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();

        builder.put(SETTING_NUMBER_OF_SHARDS, 1).build();

        builder.put(SETTING_NUMBER_OF_REPLICAS, 0).build();

        return builder.build();
    }

}
