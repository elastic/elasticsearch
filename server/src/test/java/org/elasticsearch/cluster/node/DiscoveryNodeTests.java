/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.NodeRoles.nonRemoteClusterClientNode;
import static org.elasticsearch.test.NodeRoles.remoteClusterClientNode;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class DiscoveryNodeTests extends ESTestCase {

    public void testRolesAreSorted() {
        final Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.roles()));
        final DiscoveryNode node = DiscoveryNodeUtils.create(
            "name",
            "id",
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            emptyMap(),
            roles
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
        InetAddress inetAddress = randomBoolean()
            ? InetAddress.getByName("192.0.2.1")
            : InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode node = DiscoveryNodeUtils.create("name1", "id1", transportAddress, emptyMap(), emptySet());
        assertEquals(transportAddress.address().getHostString(), node.getHostName());
        assertEquals(transportAddress.getAddress(), node.getHostAddress());
    }

    public void testDiscoveryNodeSerializationKeepsHost() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode node = DiscoveryNodeUtils.create("name1", "id1", transportAddress, emptyMap(), emptySet());

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.setTransportVersion(TransportVersion.CURRENT);
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
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));

        DiscoveryNodeRole customRole = new DiscoveryNodeRole("data_custom_role", "z", true);

        DiscoveryNode node = DiscoveryNodeUtils.create("name1", "id1", transportAddress, emptyMap(), Collections.singleton(customRole));

        {
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            streamOutput.setTransportVersion(TransportVersion.CURRENT);
            node.writeTo(streamOutput);

            StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
            in.setTransportVersion(TransportVersion.CURRENT);
            DiscoveryNode serialized = new DiscoveryNode(in);
            final Set<DiscoveryNodeRole> roles = serialized.getRoles();
            assertThat(roles, hasSize(1));
            @SuppressWarnings("OptionalGetWithoutIsPresent")
            final DiscoveryNodeRole role = roles.stream().findFirst().get();
            assertThat(role.roleName(), equalTo("data_custom_role"));
            assertThat(role.roleNameAbbreviation(), equalTo("z"));
            assertTrue(role.canContainData());
        }

        {
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            streamOutput.setTransportVersion(TransportVersion.V_7_11_0);
            node.writeTo(streamOutput);

            StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
            in.setTransportVersion(TransportVersion.V_7_11_0);
            DiscoveryNode serialized = new DiscoveryNode(in);
            final Set<DiscoveryNodeRole> roles = serialized.getRoles();
            assertThat(roles, hasSize(1));
            @SuppressWarnings("OptionalGetWithoutIsPresent")
            final DiscoveryNodeRole role = roles.stream().findFirst().get();
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

    public void testDiscoveryNodeDescriptionWithoutAttributes() {
        final DiscoveryNode node = DiscoveryNodeUtils.create(
            "test-id",
            buildNewFakeTransportAddress(),
            Map.of("test-attr", "val"),
            DiscoveryNodeRole.roles()
        );
        final StringBuilder stringBuilder = new StringBuilder();
        node.appendDescriptionWithoutAttributes(stringBuilder);
        final String descriptionWithoutAttributes = stringBuilder.toString();
        assertThat(node.toString(), allOf(startsWith(descriptionWithoutAttributes), containsString("test-attr=val")));
        assertThat(descriptionWithoutAttributes, not(containsString("test-attr")));
        assertEquals(descriptionWithoutAttributes, node.descriptionWithoutAttributes());
    }

    public void testDiscoveryNodeToXContent() {
        final TransportAddress transportAddress = buildNewFakeTransportAddress();
        final boolean withExternalId = randomBoolean();
        final DiscoveryNode node = new DiscoveryNode(
            "test-name",
            "test-id",
            "test-ephemeral-id",
            "test-hostname",
            "test-hostaddr",
            transportAddress,
            Map.of("test-attr", "val"),
            DiscoveryNodeRole.roles(),
            null,
            withExternalId ? "test-external-id" : null
        );

        assertThat(
            Strings.toString(node, true, false),
            equalTo(
                Strings.format(
                    """
                        {
                          "test-id" : {
                            "name" : "test-name",
                            "ephemeral_id" : "test-ephemeral-id",
                            "transport_address" : "%s",
                            "external_id" : "%s",
                            "attributes" : {
                              "test-attr" : "val"
                            },
                            "roles" : [
                              "data",
                              "data_cold",
                              "data_content",
                              "data_frozen",
                              "data_hot",
                              "data_warm",
                              "index",
                              "ingest",
                              "master",
                              "ml",
                              "remote_cluster_client",
                              "search",
                              "transform",
                              "voting_only"
                            ],
                            "version" : "%s",
                            "minIndexVersion" : "%s",
                            "maxIndexVersion" : "%s"
                          }
                        }""",
                    transportAddress,
                    withExternalId ? "test-external-id" : "test-name",
                    Version.CURRENT,
                    IndexVersion.MINIMUM_COMPATIBLE,
                    IndexVersion.CURRENT
                )
            )
        );
    }

    public void testDiscoveryNodeToString() {
        var node = DiscoveryNodeUtils.create(
            "test-id",
            buildNewFakeTransportAddress(),
            Map.of("test-attr", "val"),
            DiscoveryNodeRole.roles()
        );
        var toString = node.toString();

        assertThat(toString, containsString("{" + node.getId() + "}"));
        assertThat(toString, containsString("{" + node.getEphemeralId() + "}"));
        assertThat(toString, containsString("{" + node.getAddress() + "}"));
        assertThat(toString, containsString("{IScdfhilmrstvw}"));// roles
        assertThat(toString, containsString("{" + node.getVersion() + "}"));
        assertThat(toString, containsString("{test-attr=val}"));// attributes
    }
}
