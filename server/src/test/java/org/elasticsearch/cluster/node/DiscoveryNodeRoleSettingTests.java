/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.test.LambdaMatchers.falseWith;
import static org.elasticsearch.test.LambdaMatchers.trueWith;
import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.NodeRoles.removeRoles;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class DiscoveryNodeRoleSettingTests extends ESTestCase {

    public void testIsDataNode() {
        runRoleTest(DiscoveryNode::hasDataRole, DiscoveryNodeRole.DATA_ROLE);
    }

    public void testIsIngestNode() {
        runRoleTest(DiscoveryNode::isIngestNode, DiscoveryNodeRole.INGEST_ROLE);
    }

    public void testIsMasterNode() {
        runRoleTest(DiscoveryNode::isMasterNode, DiscoveryNodeRole.MASTER_ROLE);
    }

    public void testIsRemoteClusterClient() {
        runRoleTest(DiscoveryNode::isRemoteClusterClient, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
    }

    private void runRoleTest(final Predicate<Settings> predicate, final DiscoveryNodeRole role) {
        assertThat(predicate, trueWith(onlyRole(role)));
        assertThat(DiscoveryNode.getRolesFromSettings(onlyRole(role)), hasItem(role));

        assertThat(predicate, falseWith(removeRoles(Set.of(role))));
        assertThat(DiscoveryNode.getRolesFromSettings(removeRoles(Set.of(role))), not(hasItem(role)));
    }

    public void testIsDedicatedFrozenNode() {
        runRoleTest(DiscoveryNode::isDedicatedFrozenNode, DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
        assertTrue(DiscoveryNode.isDedicatedFrozenNode(addRoles(nonDataNode(), Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE))));
        assertFalse(
            DiscoveryNode.isDedicatedFrozenNode(
                addRoles(Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE, DiscoveryNodeRole.DATA_HOT_NODE_ROLE))
            )
        );
    }
}
