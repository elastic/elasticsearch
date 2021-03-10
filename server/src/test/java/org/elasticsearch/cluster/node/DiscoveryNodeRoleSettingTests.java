/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.NodeRoles.removeRoles;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class DiscoveryNodeRoleSettingTests extends ESTestCase {

    public void testIsDataNode() {
        runRoleTest(DiscoveryNode::isDataNode, DiscoveryNodeRole.DATA_ROLE);
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
        final Settings legacyTrue = Settings.builder().put(role.legacySetting().getKey(), true).build();

        assertTrue(predicate.test(legacyTrue));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{role.legacySetting()});

        assertThat(DiscoveryNode.getRolesFromSettings(legacyTrue), hasItem(role));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{role.legacySetting()});

        final Settings legacyFalse = Settings.builder().put(role.legacySetting().getKey(), false).build();

        assertFalse(predicate.test(legacyFalse));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{role.legacySetting()});

        assertThat(DiscoveryNode.getRolesFromSettings(legacyFalse), not(hasItem(role)));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{role.legacySetting()});

        assertTrue(predicate.test(onlyRole(role)));
        assertThat(DiscoveryNode.getRolesFromSettings(onlyRole(role)), hasItem(role));

        assertFalse(predicate.test(removeRoles(Set.of(role))));
        assertThat(DiscoveryNode.getRolesFromSettings(removeRoles(Set.of(role))), not(hasItem(role)));

        final Settings settings = Settings.builder().put(onlyRole(role)).put(role.legacySetting().getKey(), randomBoolean()).build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DiscoveryNode.getRolesFromSettings(settings));
        assertThat(e.getMessage(), startsWith("can not explicitly configure node roles and use legacy role setting"));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{role.legacySetting()});
    }

}
