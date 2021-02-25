/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.NodeRoles.nonRemoteClusterClientNode;
import static org.elasticsearch.test.NodeRoles.remoteClusterClientNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class DiscoveryNodeTests extends ESTestCase {

    public void testRolesAreSorted() {
        final Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
        final DiscoveryNode node = new DiscoveryNode(
            "name",
            "id",
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            emptyMap(),
            roles,
            Version.CURRENT
        );
        DiscoveryNodeRole previous = null;
        for (final DiscoveryNodeRole current : node.getRoles()) {
            if (previous != null) {
                assertThat(current, greaterThanOrEqualTo(previous));
            }
            previous = current;
        }

    }

    public void testDiscoveryNodeIsCreatedWithHostFromInetAddress() throws Exception {
        InetAddress inetAddress = randomBoolean() ? InetAddress.getByName("192.0.2.1") :
            InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, emptyMap(), emptySet(), Version.CURRENT);
        assertEquals(transportAddress.address().getHostString(), node.getHostName());
        assertEquals(transportAddress.getAddress(), node.getHostAddress());
    }

    public void testDiscoveryNodeSerializationKeepsHost() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, emptyMap(), emptySet(), Version.CURRENT);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.setVersion(Version.CURRENT);
        node.writeTo(streamOutput);

        StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
        DiscoveryNode serialized = new DiscoveryNode(in);
        assertEquals(transportAddress.address().getHostString(), serialized.getHostName());
        assertEquals(transportAddress.address().getHostString(), serialized.getAddress().address().getHostString());
        assertEquals(transportAddress.getAddress(), serialized.getHostAddress());
        assertEquals(transportAddress.getAddress(), serialized.getAddress().getAddress());
        assertEquals(transportAddress.getPort(), serialized.getAddress().getPort());
    }

    public void testDiscoveryNodeRoleWithOldVersion() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));

        DiscoveryNodeRole customRole = new DiscoveryNodeRole("data_custom_role", "z", true) {
            @Override
            public Setting<Boolean> legacySetting() {
                return null;
            }

        };

        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, emptyMap(),
            Collections.singleton(customRole), Version.CURRENT);

        {
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            streamOutput.setVersion(Version.CURRENT);
            node.writeTo(streamOutput);

            StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
            in.setVersion(Version.CURRENT);
            DiscoveryNode serialized = new DiscoveryNode(in);
            final Set<DiscoveryNodeRole> roles = serialized.getRoles();
            assertThat(roles, hasSize(1));
            @SuppressWarnings("OptionalGetWithoutIsPresent") final DiscoveryNodeRole role = roles.stream().findFirst().get();
            assertThat(role.roleName(), equalTo("data_custom_role"));
            assertThat(role.roleNameAbbreviation(), equalTo("z"));
            assertTrue(role.canContainData());
        }

        {
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            streamOutput.setVersion(Version.V_7_11_0);
            node.writeTo(streamOutput);

            StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
            in.setVersion(Version.V_7_11_0);
            DiscoveryNode serialized = new DiscoveryNode(in);
            final Set<DiscoveryNodeRole> roles = serialized.getRoles();
            assertThat(roles, hasSize(1));
            @SuppressWarnings("OptionalGetWithoutIsPresent") final DiscoveryNodeRole role = roles.stream().findFirst().get();
            assertThat(role.roleName(), equalTo("data_custom_role"));
            assertThat(role.roleNameAbbreviation(), equalTo("z"));
            assertTrue(role.canContainData());
        }

    }

    public void testDiscoveryNodeIsRemoteClusterClientDefault() {
        runTestDiscoveryNodeIsRemoteClusterClient(Settings.EMPTY, true);
    }

    public void testDiscoveryNodeIsRemoteClusterClientSet() {
        runTestDiscoveryNodeIsRemoteClusterClient(remoteClusterClientNode(), true);
    }

    public void testDiscoveryNodeIsRemoteClusterClientUnset() {
        runTestDiscoveryNodeIsRemoteClusterClient(nonRemoteClusterClientNode(), false);
    }

    private void runTestDiscoveryNodeIsRemoteClusterClient(final Settings settings, final boolean expected) {
        final DiscoveryNode node = DiscoveryNode.createLocal(settings, new TransportAddress(TransportAddress.META_ADDRESS, 9200), "node");
        assertThat(node.isRemoteClusterClient(), equalTo(expected));
        if (expected) {
            assertThat(node.getRoles(), hasItem(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
        } else {
            assertThat(node.getRoles(), not(hasItem(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE)));
        }
    }

}
