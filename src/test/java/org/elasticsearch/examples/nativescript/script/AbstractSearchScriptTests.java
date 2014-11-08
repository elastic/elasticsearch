package org.elasticsearch.examples.nativescript.script;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

/**
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public abstract class AbstractSearchScriptTests extends ElasticsearchIntegrationTest {

    @Override
    public Settings indexSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        builder.put(SETTING_NUMBER_OF_SHARDS, 1);
        builder.put(SETTING_NUMBER_OF_REPLICAS, 0);
        return builder.build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("gateway.type", "none")
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }
    
}
