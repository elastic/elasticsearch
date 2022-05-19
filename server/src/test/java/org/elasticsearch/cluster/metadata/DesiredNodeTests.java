/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DesiredNodeTests extends ESTestCase {

    public void testExternalIdIsRequired() {
        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            final String key = randomBoolean() ? NODE_NAME_SETTING.getKey() : NODE_EXTERNAL_ID_SETTING.getKey();
            if (randomBoolean()) {
                settings.put(key, "   ");
            } else {
                settings.putNull(key);
            }
        }

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DesiredNode(settings.build(), 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT)
        );
        assertThat(exception.getMessage(), is(equalTo("[node.name] or [node.external_id] is missing or empty")));
    }

    public void testExternalIdFallbacksToNodeName() {
        final String nodeName = randomAlphaOfLength(10);
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build();

        DesiredNode desiredNode = new DesiredNode(settings, 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);
        assertThat(desiredNode.externalId(), is(notNullValue()));
        assertThat(desiredNode.externalId(), is(equalTo(nodeName)));
    }

    public void testDesiredNodeMustHaveAtLeastOneProcessor() {
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10)).build();

        expectThrows(
            IllegalArgumentException.class,
            () -> new DesiredNode(settings, -1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT)
        );
    }

    public void testHasMasterRole() {
        {
            final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10)).build();

            DesiredNode desiredNode = new DesiredNode(settings, 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);
            assertTrue(desiredNode.hasMasterRole());
        }

        {
            final Settings settings = Settings.builder()
                .put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10))
                .put(NODE_ROLES_SETTING.getKey(), "master")
                .build();

            DesiredNode desiredNode = new DesiredNode(settings, 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);
            assertTrue(desiredNode.hasMasterRole());
        }

        {
            final Settings settings = Settings.builder()
                .put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10))
                .put(NODE_ROLES_SETTING.getKey(), "data_hot")
                .build();

            DesiredNode desiredNode = new DesiredNode(settings, 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);
            assertFalse(desiredNode.hasMasterRole());
        }
    }

    public void testGetRoles() {
        final var settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10));

        final var role = randomBoolean() ? null : randomValueOtherThan(VOTING_ONLY_NODE_ROLE, () -> randomFrom(DiscoveryNodeRole.roles()));
        if (role != null) {
            settings.put(NODE_ROLES_SETTING.getKey(), role.roleName());
        }

        final var desiredNode = new DesiredNode(settings.build(), 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);

        if (role != null) {
            assertThat(desiredNode.getRoles(), hasSize(1));
            assertThat(desiredNode.getRoles(), contains(role));
        } else {
            assertThat(desiredNode.getRoles(), contains(NODE_ROLES_SETTING.get(Settings.EMPTY).toArray()));
        }
    }
}
