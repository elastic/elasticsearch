/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.elasticsearch.test.NodeRoles.removeRoles;
import static org.hamcrest.Matchers.equalTo;

public class ConnectionProfileTests extends ESTestCase {

    public void testBuildConnectionProfile() {
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        TimeValue connectTimeout = TimeValue.timeValueMillis(randomIntBetween(1, 10));
        TimeValue handshakeTimeout = TimeValue.timeValueMillis(randomIntBetween(1, 10));
        TimeValue pingInterval = TimeValue.timeValueMillis(randomIntBetween(1, 10));
        Compression.Enabled compressionEnabled =
            randomFrom(Compression.Enabled.TRUE, Compression.Enabled.FALSE, Compression.Enabled.INDEXING_DATA);
        Compression.Scheme compressionScheme =
            randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4);
        final boolean setConnectTimeout = randomBoolean();
        if (setConnectTimeout) {
            builder.setConnectTimeout(connectTimeout);
        }
        final boolean setHandshakeTimeout = randomBoolean();
        if (setHandshakeTimeout) {
            builder.setHandshakeTimeout(handshakeTimeout);
        }

        final boolean setCompress = randomBoolean();
        if (setCompress) {
            builder.setCompressionEnabled(compressionEnabled);
        }

        final boolean setCompressionScheme = randomBoolean();
        if (setCompressionScheme) {
            builder.setCompressionScheme(compressionScheme);
        }
        final boolean setPingInterval = randomBoolean();
        if (setPingInterval) {
            builder.setPingInterval(pingInterval);
        }
        builder.addConnections(1, TransportRequestOptions.Type.BULK);
        builder.addConnections(2, TransportRequestOptions.Type.STATE, TransportRequestOptions.Type.RECOVERY);
        builder.addConnections(3, TransportRequestOptions.Type.PING);
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class, builder::build);
        assertEquals("not all types are added for this connection profile - missing types: [REG]", illegalStateException.getMessage());

        IllegalArgumentException illegalArgumentException = expectThrows(IllegalArgumentException.class,
            () -> builder.addConnections(4, TransportRequestOptions.Type.REG, TransportRequestOptions.Type.PING));
        assertEquals("type [PING] is already registered", illegalArgumentException.getMessage());
        builder.addConnections(4, TransportRequestOptions.Type.REG);
        ConnectionProfile build = builder.build();
        if (randomBoolean()) {
            build = new ConnectionProfile.Builder(build).build();
        }
        assertEquals(10, build.getNumConnections());
        if (setConnectTimeout) {
            assertEquals(connectTimeout, build.getConnectTimeout());
        } else {
            assertNull(build.getConnectTimeout());
        }

        if (setHandshakeTimeout) {
            assertEquals(handshakeTimeout, build.getHandshakeTimeout());
        } else {
            assertNull(build.getHandshakeTimeout());
        }

        if (setCompress) {
            assertEquals(compressionEnabled, build.getCompressionEnabled());
        } else {
            assertNull(build.getCompressionEnabled());
        }

        if (setCompressionScheme) {
            assertEquals(compressionScheme, build.getCompressionScheme());
        } else {
            assertNull(build.getCompressionScheme());
        }

        if (setPingInterval) {
            assertEquals(pingInterval, build.getPingInterval());
        } else {
            assertNull(build.getPingInterval());
        }

        List<Integer> list = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        final int numIters = randomIntBetween(5, 10);
        assertEquals(4, build.getHandles().size());
        assertEquals(0, build.getHandles().get(0).offset);
        assertEquals(1, build.getHandles().get(0).length);
        assertEquals(EnumSet.of(TransportRequestOptions.Type.BULK), build.getHandles().get(0).getTypes());
        Integer channel = build.getHandles().get(0).getChannel(list);
        for (int i = 0; i < numIters; i++) {
            assertEquals(0, channel.intValue());
        }

        assertEquals(1, build.getHandles().get(1).offset);
        assertEquals(2, build.getHandles().get(1).length);
        assertEquals(EnumSet.of(TransportRequestOptions.Type.STATE, TransportRequestOptions.Type.RECOVERY),
            build.getHandles().get(1).getTypes());
        channel = build.getHandles().get(1).getChannel(list);
        for (int i = 0; i < numIters; i++) {
            assertThat(channel, Matchers.anyOf(Matchers.is(1), Matchers.is(2)));
        }

        assertEquals(3, build.getHandles().get(2).offset);
        assertEquals(3, build.getHandles().get(2).length);
        assertEquals(EnumSet.of(TransportRequestOptions.Type.PING), build.getHandles().get(2).getTypes());
        channel = build.getHandles().get(2).getChannel(list);
        for (int i = 0; i < numIters; i++) {
            assertThat(channel, Matchers.anyOf(Matchers.is(3), Matchers.is(4), Matchers.is(5)));
        }

        assertEquals(6, build.getHandles().get(3).offset);
        assertEquals(4, build.getHandles().get(3).length);
        assertEquals(EnumSet.of(TransportRequestOptions.Type.REG), build.getHandles().get(3).getTypes());
        channel = build.getHandles().get(3).getChannel(list);
        for (int i = 0; i < numIters; i++) {
            assertThat(channel, Matchers.anyOf(Matchers.is(6), Matchers.is(7), Matchers.is(8), Matchers.is(9)));
        }

        assertEquals(3, build.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(4, build.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(2, build.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(2, build.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(1, build.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));
    }

    public void testNoChannels() {
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(1, TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.STATE,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG);
        builder.addConnections(0, TransportRequestOptions.Type.PING);
        ConnectionProfile build = builder.build();
        List<Integer> array = Collections.singletonList(0);
        assertEquals(Integer.valueOf(0), build.getHandles().get(0).getChannel(array));
        expectThrows(IllegalStateException.class, () -> build.getHandles().get(1).getChannel(array));
    }

    public void testConnectionProfileResolve() {
        final ConnectionProfile defaultProfile = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
        assertEquals(defaultProfile, ConnectionProfile.resolveConnectionProfile(null, defaultProfile));

        final ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.BULK);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.RECOVERY);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.REG);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.STATE);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.PING);

        final boolean connectionTimeoutSet = randomBoolean();
        if (connectionTimeoutSet) {
            builder.setConnectTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        final boolean connectionHandshakeSet = randomBoolean();
        if (connectionHandshakeSet) {
            builder.setHandshakeTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        final boolean pingIntervalSet = randomBoolean();
        if (pingIntervalSet) {
            builder.setPingInterval(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        final boolean connectionCompressSet = randomBoolean();
        if (connectionCompressSet) {
            Compression.Enabled compressionEnabled =
                randomFrom(Compression.Enabled.TRUE, Compression.Enabled.FALSE, Compression.Enabled.INDEXING_DATA);
            builder.setCompressionEnabled(compressionEnabled);
        }
        final boolean connectionCompressionScheme = randomBoolean();
        if (connectionCompressionScheme) {
            Compression.Scheme compressionScheme =
                randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4);
            builder.setCompressionScheme(compressionScheme);
        }

        final ConnectionProfile profile = builder.build();
        final ConnectionProfile resolved = ConnectionProfile.resolveConnectionProfile(profile, defaultProfile);
        assertNotEquals(resolved, defaultProfile);
        assertThat(resolved.getNumConnections(), equalTo(profile.getNumConnections()));
        assertThat(resolved.getHandles(), equalTo(profile.getHandles()));

        assertThat(resolved.getConnectTimeout(),
            equalTo(connectionTimeoutSet ? profile.getConnectTimeout() : defaultProfile.getConnectTimeout()));
        assertThat(resolved.getHandshakeTimeout(),
            equalTo(connectionHandshakeSet ? profile.getHandshakeTimeout() : defaultProfile.getHandshakeTimeout()));
        assertThat(resolved.getPingInterval(),
            equalTo(pingIntervalSet ? profile.getPingInterval() : defaultProfile.getPingInterval()));
        assertThat(resolved.getCompressionEnabled(),
            equalTo(connectionCompressSet ? profile.getCompressionEnabled() : defaultProfile.getCompressionEnabled()));
        assertThat(resolved.getCompressionScheme(),
            equalTo(connectionCompressionScheme ? profile.getCompressionScheme() :
                defaultProfile.getCompressionScheme()));
    }

    public void testDefaultConnectionProfile() {
        ConnectionProfile profile = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
        assertEquals(13, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(2, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));
        assertEquals(TransportSettings.CONNECT_TIMEOUT.get(Settings.EMPTY), profile.getConnectTimeout());
        assertEquals(TransportSettings.CONNECT_TIMEOUT.get(Settings.EMPTY), profile.getHandshakeTimeout());
        assertEquals(TransportSettings.TRANSPORT_COMPRESS.get(Settings.EMPTY), profile.getCompressionEnabled());
        assertEquals(TransportSettings.TRANSPORT_COMPRESSION_SCHEME.get(Settings.EMPTY), profile.getCompressionScheme());
        assertEquals(TransportSettings.PING_SCHEDULE.get(Settings.EMPTY), profile.getPingInterval());

        profile = ConnectionProfile.buildDefaultConnectionProfile(nonMasterNode());
        assertEquals(12, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(2, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));

        profile = ConnectionProfile.buildDefaultConnectionProfile(nonDataNode());
        assertEquals(11, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));

        profile = ConnectionProfile.buildDefaultConnectionProfile(
            removeRoles(nonDataNode(), Set.of(DiscoveryNodeRole.MASTER_ROLE))
        );
        assertEquals(10, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));
    }
}
