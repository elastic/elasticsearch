/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;
import org.elasticsearch.xpack.security.action.rolemapping.ReservedRoleMappingAction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.integration.RoleMappingFileSettingsIT.setupClusterStateListener;
import static org.elasticsearch.integration.RoleMappingFileSettingsIT.setupClusterStateListenerForCleanup;
import static org.elasticsearch.integration.RoleMappingFileSettingsIT.writeJSONFile;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class FileSettingsRoleMappingsRestartIT extends SecurityIntegTestCase {

    private static AtomicLong versionCounter = new AtomicLong(1);

    private static String testJSONOnlyRoleMappings = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "role_mappings": {
                       "everyone_kibana_alone": {
                          "enabled": true,
                          "roles": [ "kibana_user" ],
                          "rules": { "field": { "username": "*" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                             "_foo": "something"
                          }
                       },
                       "everyone_fleet_alone": {
                          "enabled": false,
                          "roles": [ "fleet_user" ],
                          "rules": { "field": { "username": "*" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be3-bb8d-02bb270cb3a7",
                             "_foo": "something_else"
                          }
                       }
                 }
             }
        }""";

    private static String emptyJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                "cluster_settings": {},
                "role_mappings": {}
             }
        }""";

    public void testReservedStatePersistsOnRestart() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);

        final String masterNode = internalCluster().getMasterName();
        var savedClusterState = setupClusterStateListener(masterNode, "everyone_kibana_alone");

        awaitFileSettingsWatcher();
        logger.info("--> write some role mappings, no other file settings");
        writeJSONFile(masterNode, testJSONOnlyRoleMappings, logger, versionCounter);
        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        var clusterState = clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet().getState();
        assertRoleMappingReservedMetadata(clusterState, "everyone_kibana_alone", "everyone_fleet_alone");
        List<ExpressionRoleMapping> roleMappings = new ArrayList<>(RoleMappingMetadata.getFromClusterState(clusterState).getRoleMappings());
        assertThat(
            roleMappings,
            containsInAnyOrder(
                new ExpressionRoleMapping(
                    "everyone_kibana_alone",
                    new FieldExpression("username", List.of(new FieldExpression.FieldValue("*"))),
                    List.of("kibana_user"),
                    List.of(),
                    Map.of("uuid", "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7", "_foo", "something"),
                    true
                ),
                new ExpressionRoleMapping(
                    "everyone_fleet_alone",
                    new FieldExpression("username", List.of(new FieldExpression.FieldValue("*"))),
                    List.of("fleet_user"),
                    List.of(),
                    Map.of("uuid", "b9a59ba9-6b92-4be3-bb8d-02bb270cb3a7", "_foo", "something_else"),
                    false
                )
            )
        );

        logger.info("--> restart master");
        internalCluster().restartNode(masterNode);
        ensureGreen();

        // assert role mappings are recovered from "disk"
        clusterState = clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet().getState();
        assertRoleMappingReservedMetadata(clusterState, "everyone_kibana_alone", "everyone_fleet_alone");
        roleMappings = new ArrayList<>(RoleMappingMetadata.getFromClusterState(clusterState).getRoleMappings());
        assertThat(
            roleMappings,
            containsInAnyOrder(
                new ExpressionRoleMapping(
                    "everyone_kibana_alone",
                    new FieldExpression("username", List.of(new FieldExpression.FieldValue("*"))),
                    List.of("kibana_user"),
                    List.of(),
                    Map.of("uuid", "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7", "_foo", "something"),
                    true
                ),
                new ExpressionRoleMapping(
                    "everyone_fleet_alone",
                    new FieldExpression("username", List.of(new FieldExpression.FieldValue("*"))),
                    List.of("fleet_user"),
                    List.of(),
                    Map.of("uuid", "b9a59ba9-6b92-4be3-bb8d-02bb270cb3a7", "_foo", "something_else"),
                    false
                )
            )
        );

        // now remove the role mappings via the same settings file
        savedClusterState = setupClusterStateListenerForCleanup(masterNode);
        awaitFileSettingsWatcher();
        logger.info("--> remove the role mappings with an empty settings file");
        writeJSONFile(masterNode, emptyJSON, logger, versionCounter);
        awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        clusterState = clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet().getState();
        assertRoleMappingReservedMetadata(clusterState);
        roleMappings = new ArrayList<>(RoleMappingMetadata.getFromClusterState(clusterState).getRoleMappings());
        assertThat(roleMappings, emptyIterable());

        // and restart the master to confirm the role mappings are all gone
        logger.info("--> restart master again");
        internalCluster().restartNode(masterNode);
        ensureGreen();

        // assert empty role mappings are recovered from "disk"
        clusterState = clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet().getState();
        assertRoleMappingReservedMetadata(clusterState);
        roleMappings = new ArrayList<>(RoleMappingMetadata.getFromClusterState(clusterState).getRoleMappings());
        assertThat(roleMappings, emptyIterable());
    }

    private void assertRoleMappingReservedMetadata(ClusterState clusterState, String... names) {
        assertThat(
            clusterState.metadata()
                .reservedStateMetadata()
                .get(FileSettingsService.NAMESPACE)
                .handlers()
                .get(ReservedRoleMappingAction.NAME)
                .keys(),
            containsInAnyOrder(names)
        );
    }

    private void awaitFileSettingsWatcher() throws Exception {
        final String masterNode = internalCluster().getMasterName();
        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);
        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
    }
}
