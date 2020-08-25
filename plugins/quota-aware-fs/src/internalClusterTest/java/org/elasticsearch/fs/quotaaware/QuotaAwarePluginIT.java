package org.elasticsearch.fs.quotaaware;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.List;

public class QuotaAwarePluginIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(QuotaAwareFsPlugin.class);
    }

    /**
     * Check that the plugin is not active, since it is a bootstrap-only plugin.
     */
    public void testPluginShouldNotBeActive() {
        final Client client = this.client();

        final NodesInfoResponse response = client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();

        final boolean hasPlugin = response.getNodes()
            .stream()
            .flatMap(node -> node.getInfo(PluginsAndModules.class).getPluginInfos().stream())
            .noneMatch(pluginInfo -> pluginInfo.getName().equals("quota-aware-fs"));

        assertFalse("quota-aware-fs plugin should not be active", hasPlugin);
    }
}
