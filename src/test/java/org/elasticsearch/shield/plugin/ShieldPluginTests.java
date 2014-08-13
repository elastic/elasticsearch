/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.plugin;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.os.OsUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.transport.SecuredTransportService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportModule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.headerValue;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 2, randomDynamicTemplates = false)
public class ShieldPluginTests extends ElasticsearchIntegrationTest {


    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        File folder = newFolder();
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put("plugin.types", SecurityPlugin.class.getName())
                .put("shield.audit.enabled", true)
                .put("shield.authc.esusers.files.users", copyFile(folder, "users"))
                .put("shield.authc.esusers.files.users_roles", copyFile(folder, "users_roles"))
                .put("shield.authz.store.files.roles", copyFile(folder, "roles.yml"))
                .put("shield.n2n.file", copyFile(folder, "ip_filter.yml"))
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, SecuredTransportService.class.getName())
                        // for the test internal node clients
                .put("request.headers.Authorization", headerValue("test_user", "changeme".toCharArray()));

        if (OsUtils.MAC) {
            builder.put("network.host", randomBoolean() ? "127.0.0.1" : "::1");
        }

        return builder.build();
    }


    @Override
    protected Settings transportClientSettings() {
        return ImmutableSettings.builder()
                .put("request.headers.Authorization", headerValue("test_user", "changeme".toCharArray()))
                .build();
    }

    @Test
    @TestLogging("_root:INFO,plugins.PluginsService:TRACE")
    public void testThatPluginIsLoaded() {
        logger.info("--> Getting nodes info");
        NodesInfoResponse nodeInfos = internalCluster().transportClient().admin().cluster().prepareNodesInfo().get();
        logger.info("--> Checking nodes info");
        for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
            assertThat(nodeInfo.getPlugins().getInfos(), hasSize(1));
            assertThat(nodeInfo.getPlugins().getInfos().get(0).getName(), is(SecurityPlugin.NAME));
        }
    }

    private File newFolder() {
        try {
            return tmpFolder.newFolder();
        } catch (IOException ioe) {
            logger.error("could not create temporary folder", ioe);
            fail("could not create temporary folder");
            return null;
        }
    }

    private String copyFile(File folder, String name) {
        Path file = folder.toPath().resolve(name);
        try {
            Files.copy(getClass().getResourceAsStream(name), file);
        } catch (IOException ioe) {
            logger.error("could not copy temporary configuration file [" + name + "]", ioe);
            fail("could not copy temporary configuration file [" + name + "]");
        }
        return file.toAbsolutePath().toString();
    }

}
