/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLService;

import static org.hamcrest.Matchers.containsString;

public class XPackPluginTests extends ESTestCase {

    public void testXPackInstalledAttrClashOnTransport() throws Exception {
        Settings.Builder builder = Settings.builder();
        builder.put("node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR, "true");
        builder.put(Client.CLIENT_TYPE_SETTING_S.getKey(), "transport");
        XPackPlugin xpackPlugin = createXPackPlugin(builder.put("path.home", createTempDir()).build());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, xpackPlugin::additionalSettings);
        assertThat(e.getMessage(),
            containsString("Directly setting [node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR + "] is not permitted"));
    }

    public void testXPackInstalledAttrClashOnNode() throws Exception {
        Settings.Builder builder = Settings.builder();
        builder.put("node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR, "false");
        XPackPlugin xpackPlugin = createXPackPlugin(builder.put("path.home", createTempDir()).build());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, xpackPlugin::additionalSettings);
        assertThat(e.getMessage(),
            containsString("Conflicting setting [node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR + "]"));
    }

    public void testXPackInstalledAttrExists() throws Exception {
        XPackPlugin xpackPlugin = createXPackPlugin(Settings.builder().put("path.home", createTempDir()).build());
        assertEquals("true", xpackPlugin.additionalSettings().get("node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR));
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
