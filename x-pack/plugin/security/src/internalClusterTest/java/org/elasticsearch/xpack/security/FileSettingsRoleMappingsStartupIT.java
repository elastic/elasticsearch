/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.integration.RoleMappingFileSettingsIT.setupClusterStateListenerForError;
import static org.elasticsearch.integration.RoleMappingFileSettingsIT.writeJSONFile;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class FileSettingsRoleMappingsStartupIT extends SecurityIntegTestCase {

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

    @Override
    protected void doAssertXPackIsInstalled() {}

    @Override
    protected Path nodeConfigPath(int nodeOrdinal) {
        return null;
    }

    @TestLogging(
        value = "org.elasticsearch.common.file:DEBUG,org.elasticsearch.xpack.security:DEBUG,org.elasticsearch.cluster.metadata:DEBUG",
        reason = "https://github.com/elastic/elasticsearch/issues/98391"
    )
    public void testFailsOnStartMasterNodeWithError() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startMasterOnlyNode();

        var savedClusterState = setupClusterStateListenerForError(
            internalCluster().getCurrentMasterNodeInstance(ClusterService.class),
            errorMetadata -> {
                assertEquals(ReservedStateErrorMetadata.ErrorKind.VALIDATION, errorMetadata.errorKind());
                assertThat(errorMetadata.errors(), allOf(notNullValue(), hasSize(1)));
                assertThat(errorMetadata.errors().get(0), containsString("Fake exception"));
            }
        );
        logger.info("--> write some role mappings, no other file settings");
        writeJSONFile(internalCluster().getMasterName(), testJSONForFailedCase, logger, versionCounter);

        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);
    }

    public Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            UnstableLocalStateSecurity.class,
            Netty4Plugin.class,
            ReindexPlugin.class,
            CommonAnalysisPlugin.class,
            InternalSettingsPlugin.class,
            MapperExtrasPlugin.class,
            Wildcard.class
        );
    }
}
