/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.TokenMetadata;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.Collections;
import java.util.Map;

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
            final Map<String, String> attributes;
            if (randomBoolean()) {
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

    private XPackPlugin createXPackPlugin(Settings settings) throws Exception {
        return new XPackPlugin(settings, null){

            @Override
            protected void setSslService(SSLService sslService) {
                // disable
            }

            @Override
            protected void setLicenseState(XPackLicenseState licenseState) {
                // disable
            }
        };
    }

}
