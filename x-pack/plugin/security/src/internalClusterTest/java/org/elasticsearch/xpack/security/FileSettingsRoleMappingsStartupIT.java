/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.NodeRoles.dataOnlyNode;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class FileSettingsRoleMappingsStartupIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return applyWorkaroundForIssue92812(super.nodeSettings(nodeOrdinal, otherSettings));
    }

    private static AtomicLong versionCounter = new AtomicLong(1);
    private static String testJSONForFailedCase = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "role_mappings": {
                       "everyone_kibana_2": {
                          "enabled": true,
                          "roles": [ "kibana_user" ],
                          "rules": { "field": { "username": "*" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                             "_foo": "something"
                          }
                       }
                 }
             }
        }""";

    private void writeJSONFile(String node, String json) throws Exception {
        long version = versionCounter.incrementAndGet();

        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);

        Files.deleteIfExists(fileSettingsService.operatorSettingsFile());

        Files.createDirectories(fileSettingsService.operatorSettingsDir());
        Path tempFilePath = createTempFile();

        logger.info("--> writing JSON config to node {} with path {}", node, tempFilePath);
        logger.info(Strings.format(json, version));
        Files.write(tempFilePath, Strings.format(json, version).getBytes(StandardCharsets.UTF_8));
        Files.move(tempFilePath, fileSettingsService.operatorSettingsFile(), StandardCopyOption.ATOMIC_MOVE);
    }

    public void testFailsOnStartMasterNodeWithError() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);

        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        logger.info("--> write some role mappings, no other file settings");
        writeJSONFile(dataNode, testJSONForFailedCase);

        logger.info("--> stop data node");
        internalCluster().stopNode(dataNode);
        logger.info("--> start master node");
        assertEquals(
            "unable to launch a new watch service",
            expectThrows(IllegalStateException.class, () -> internalCluster().startMasterOnlyNode()).getMessage()
        );
    }

    public Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            UnstableLocalStateSecurity.class,
            Netty4Plugin.class,
            ReindexPlugin.class,
            CommonAnalysisPlugin.class,
            InternalSettingsPlugin.class,
            MapperExtrasPlugin.class
        );
    }

    @Override
    protected boolean addMockTransportService() {
        return false; // security has its own transport service
    }
}
