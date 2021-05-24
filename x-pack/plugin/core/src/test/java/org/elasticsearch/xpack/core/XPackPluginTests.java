/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xpack.core.security.authc.TokenMetadata;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.hamcrest.Matchers.containsString;

public class XPackPluginTests extends ESTestCase {

    public void testXPackInstalledAttrClash() throws Exception {
        Settings.Builder builder = Settings.builder();
        builder.put("node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR, randomBoolean());
        if (randomBoolean()) {
            builder.put(Client.CLIENT_TYPE_SETTING_S.getKey(), "transport");
        }
        XPackPlugin xpackPlugin = createXPackPlugin(builder.put("path.home", createTempDir()).build());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, xpackPlugin::additionalSettings);
        assertThat(e.getMessage(),
            containsString("Directly setting [node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR + "] is not permitted"));
    }

    public void testXPackInstalledAttrExists() throws Exception {
        XPackPlugin xpackPlugin = createXPackPlugin(Settings.builder().put("path.home", createTempDir()).build());
        assertEquals("true", xpackPlugin.additionalSettings().get("node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR));
    }

    public void testNodesNotReadyForXPackCustomMetadata() {
        boolean compatible;
        boolean nodesCompatible = true;
        DiscoveryNodes.Builder discoveryNodes = DiscoveryNodes.builder();

        for (int i = 0; i < randomInt(3); i++) {
            final Version version = VersionUtils.randomVersion(random());
            final Map<String, String> attributes;
            if (randomBoolean() && version.onOrAfter(Version.V_6_3_0)) {
                attributes = Collections.singletonMap(XPackPlugin.XPACK_INSTALLED_NODE_ATTR, "true");
            } else {
                nodesCompatible = false;
                attributes = Collections.emptyMap();
            }

            discoveryNodes.add(new DiscoveryNode("node_" + i, buildNewFakeTransportAddress(), attributes, Collections.emptySet(),
                Version.CURRENT));
        }
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterName.DEFAULT);

        if (randomBoolean()) {
            clusterStateBuilder.putCustom(TokenMetadata.TYPE, new TokenMetadata(Collections.emptyList(), new byte[0]));
            compatible = true;
        } else {
            compatible = nodesCompatible;
        }

        ClusterState clusterState = clusterStateBuilder.nodes(discoveryNodes.build()).build();

        assertEquals(XPackPlugin.nodesNotReadyForXPackCustomMetadata(clusterState).isEmpty(), nodesCompatible);
        assertEquals(XPackPlugin.isReadyForXPackCustomMetadata(clusterState), compatible);

        if (compatible == false) {
            IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> XPackPlugin.checkReadyForXPackCustomMetadata(clusterState));
            assertThat(e.getMessage(), containsString("The following nodes are not ready yet for enabling x-pack custom metadata:"));
        }
    }

    public void testCustomRestWrapperDeprecationMessage() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("path.home", createTempDir())
            .build();
        XPackPlugin xpackPlugin = createXPackPlugin(settings);
        SettingsModule moduleSettings = new SettingsModule(Settings.EMPTY);

        ThreadPool threadPool = new TestThreadPool("testCustomRestWrapperDeprecationMessage");
        try {
            UsageService usageService = new UsageService();
            new ActionModule(false, moduleSettings.getSettings(),
                TestIndexNameExpressionResolver.newInstance(),
                moduleSettings.getIndexScopedSettings(), moduleSettings.getClusterSettings(), moduleSettings.getSettingsFilter(),
                threadPool, singletonList(xpackPlugin), null, null, usageService, null);
            assertWarnings("The org.elasticsearch.xpack.core.XPackPluginTests$1 plugin installs a custom REST wrapper. " +
                "This functionality is deprecated and will not be possible in Elasticsearch 8.0. If this plugin is intended to provide " +
                "security features for Elasticsearch then you should switch to using the built-in Elasticsearch features instead.");
        } finally {
            threadPool.shutdown();
        }
    }

    class FakeHandler implements RestHandler {
        @Override
        public List<Route> routes() {
            return singletonList(new Route(GET, "/_dummy"));
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        }
    }

    private XPackPlugin createXPackPlugin (Settings settings) throws Exception {
        return new XPackPlugin(settings, null) {

            @Override
            protected void setSslService(SSLService sslService) {
                // disable
            }

            @Override
            protected void setLicenseState(XPackLicenseState licenseState) {
                // disable
            }

            @Override
            public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
                return handler -> new FakeHandler();
            }
        };
    }
}
