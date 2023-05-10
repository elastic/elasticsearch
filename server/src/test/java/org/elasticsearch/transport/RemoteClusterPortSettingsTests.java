/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterPortSettingsTests extends ESTestCase {
    /**
     * Tests that, if Remote Cluster Security 2.0 is enabled, we reject any configuration of that profile
     * via the profile settings.
     */
    public void testRemoteClusterProfileCannotBeUsedWhenRcs2IsEnabled() {
        List<Setting.AffixSetting<?>> transportProfileSettings = List.of(
            TransportSettings.TCP_KEEP_ALIVE_PROFILE,
            TransportSettings.TCP_KEEP_IDLE_PROFILE,
            TransportSettings.TCP_KEEP_INTERVAL_PROFILE,
            TransportSettings.TCP_KEEP_COUNT_PROFILE,
            TransportSettings.TCP_NO_DELAY_PROFILE,
            TransportSettings.TCP_REUSE_ADDRESS_PROFILE,
            TransportSettings.TCP_SEND_BUFFER_SIZE_PROFILE,
            TransportSettings.TCP_RECEIVE_BUFFER_SIZE_PROFILE,
            TransportSettings.BIND_HOST_PROFILE,
            TransportSettings.PUBLISH_HOST_PROFILE,
            TransportSettings.PORT_PROFILE,
            TransportSettings.PUBLISH_PORT_PROFILE
        );

        for (Setting.AffixSetting<?> profileSetting : transportProfileSettings) {
            Settings testSettings = Settings.builder()
                .put(REMOTE_CLUSTER_SERVER_ENABLED.getKey(), true)
                // We can just stick a random value in, even if it doesn't match the type - that validation happens at a different layer
                .put(profileSetting.getConcreteSettingForNamespace(REMOTE_CLUSTER_PROFILE).getKey(), randomAlphaOfLength(5))
                .build();
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RemoteClusterPortSettings.buildRemoteAccessProfileSettings(testSettings)
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "Remote Access settings should not be configured using the [_remote_cluster] profile. "
                        + "Use the [remote_cluster.] settings instead."
                )
            );
        }
    }

    public void testPortSettingsConstruction() {
        String hostValue = NetworkAddress.format(randomIp(true));
        Settings.Builder testSettingsBuilder = Settings.builder()
            .put(REMOTE_CLUSTER_SERVER_ENABLED.getKey(), true)
            .put(randomFrom(RemoteClusterPortSettings.HOST, TransportSettings.BIND_HOST, TransportSettings.HOST).getKey(), hostValue);

        boolean publishHostSet = randomBoolean();
        String publishHostValue = publishHostSet ? NetworkAddress.format(randomIp(true)) : hostValue;
        if (publishHostSet) {
            testSettingsBuilder.put(RemoteClusterPortSettings.PUBLISH_HOST.getKey(), publishHostValue);
        }

        boolean bindHostSet = randomBoolean();
        String bindHostValue = bindHostSet ? NetworkAddress.format(randomIp(true)) : hostValue;
        if (bindHostSet) {
            testSettingsBuilder.put(RemoteClusterPortSettings.BIND_HOST.getKey(), bindHostValue);
        }

        boolean portSet = randomBoolean();
        int portValue = portSet ? randomInt(65535) : RemoteClusterPortSettings.PORT.getDefault(Settings.EMPTY);
        if (portSet) {
            testSettingsBuilder.put(RemoteClusterPortSettings.PORT.getKey(), portValue);
        }

        boolean publishPortSet = randomBoolean();
        int publishPortValue = publishPortSet ? randomInt(65535) : -1; // Publish port is handled a bit weird in ProfileSettings
        if (publishPortSet) {
            testSettingsBuilder.put(RemoteClusterPortSettings.PUBLISH_PORT.getKey(), publishPortValue);
        }

        boolean keepAliveSet = randomBoolean();
        boolean keepAliveValue = keepAliveSet ? randomBoolean() : NetworkService.TCP_KEEP_ALIVE.getDefault(Settings.EMPTY);
        if (keepAliveSet) {
            testSettingsBuilder.put(
                randomFrom(RemoteClusterPortSettings.TCP_KEEP_ALIVE, TransportSettings.TCP_KEEP_ALIVE, NetworkService.TCP_KEEP_ALIVE)
                    .getKey(),
                keepAliveValue
            );
        }
        boolean keepIdleSet = randomBoolean();
        int keepIdleValue = keepIdleSet ? randomInt(300) : NetworkService.TCP_KEEP_IDLE.getDefault(Settings.EMPTY);
        if (keepIdleSet) {
            testSettingsBuilder.put(
                randomFrom(RemoteClusterPortSettings.TCP_KEEP_IDLE, TransportSettings.TCP_KEEP_IDLE, NetworkService.TCP_KEEP_IDLE).getKey(),
                keepIdleValue
            );
        }

        boolean keepIntervalSet = randomBoolean();
        int keepIntervalValue = keepIntervalSet ? randomInt(300) : NetworkService.TCP_KEEP_INTERVAL.getDefault(Settings.EMPTY);
        if (keepIntervalSet) {
            testSettingsBuilder.put(
                randomFrom(
                    RemoteClusterPortSettings.TCP_KEEP_INTERVAL,
                    TransportSettings.TCP_KEEP_INTERVAL,
                    NetworkService.TCP_KEEP_INTERVAL
                ).getKey(),
                keepIntervalValue
            );
        }
        boolean keepCountSet = randomBoolean();
        int keepCountValue = keepCountSet ? randomInt(1000000) : NetworkService.TCP_KEEP_COUNT.getDefault(Settings.EMPTY);
        if (keepCountSet) {
            testSettingsBuilder.put(
                randomFrom(RemoteClusterPortSettings.TCP_KEEP_COUNT, TransportSettings.TCP_KEEP_COUNT, NetworkService.TCP_KEEP_COUNT)
                    .getKey(),
                keepCountValue
            );
        }
        boolean noDelaySet = randomBoolean();
        boolean noDelayValue = noDelaySet ? randomBoolean() : NetworkService.TCP_NO_DELAY.getDefault(Settings.EMPTY);
        if (noDelaySet) {
            testSettingsBuilder.put(
                randomFrom(RemoteClusterPortSettings.TCP_NO_DELAY, TransportSettings.TCP_NO_DELAY).getKey(),
                noDelayValue
            );
        }
        boolean reuseAddressSet = randomBoolean();
        boolean reuseAddressValue = reuseAddressSet ? randomBoolean() : NetworkService.TCP_REUSE_ADDRESS.getDefault(Settings.EMPTY);
        if (reuseAddressSet) {
            testSettingsBuilder.put(
                randomFrom(
                    RemoteClusterPortSettings.TCP_REUSE_ADDRESS,
                    TransportSettings.TCP_REUSE_ADDRESS,
                    NetworkService.TCP_REUSE_ADDRESS
                ).getKey(),
                reuseAddressValue
            );
        }
        boolean sendBufferSizeSet = randomBoolean();
        int sendBufSizeBytes = randomInt(10_000_000);
        ByteSizeValue sendBufferSizeValue = sendBufferSizeSet
            ? ByteSizeValue.ofBytes(sendBufSizeBytes)
            : NetworkService.TCP_SEND_BUFFER_SIZE.getDefault(Settings.EMPTY);
        if (sendBufferSizeSet) {
            testSettingsBuilder.put(
                randomFrom(
                    RemoteClusterPortSettings.TCP_SEND_BUFFER_SIZE,
                    TransportSettings.TCP_SEND_BUFFER_SIZE,
                    NetworkService.TCP_SEND_BUFFER_SIZE
                ).getKey(),
                sendBufferSizeValue
            );
        }
        boolean receiveBufferSizeSet = randomBoolean();
        int rcvBufSizeBytes = randomInt(10_000_000);
        ByteSizeValue receiveBufferSizeValue = receiveBufferSizeSet
            ? ByteSizeValue.ofBytes(rcvBufSizeBytes)
            : NetworkService.TCP_RECEIVE_BUFFER_SIZE.getDefault(Settings.EMPTY);
        if (receiveBufferSizeSet) {
            testSettingsBuilder.put(
                randomFrom(
                    RemoteClusterPortSettings.TCP_RECEIVE_BUFFER_SIZE,
                    TransportSettings.TCP_RECEIVE_BUFFER_SIZE,
                    NetworkService.TCP_RECEIVE_BUFFER_SIZE
                ).getKey(),
                receiveBufferSizeValue
            );
        }

        Settings testSettings = testSettingsBuilder.build();
        TcpTransport.ProfileSettings profileSettings = RemoteClusterPortSettings.buildRemoteAccessProfileSettings(testSettings);
        assertThat(profileSettings.profileName, equalTo(REMOTE_CLUSTER_PROFILE));
        assertThat(profileSettings.bindHosts, contains(bindHostValue));
        assertThat(profileSettings.publishHosts, contains(publishHostValue));
        assertThat(profileSettings.portOrRange, equalTo(Integer.toString(portValue)));
        assertThat(profileSettings.publishPort, equalTo(publishPortValue));

        assertThat(profileSettings.tcpNoDelay, equalTo(noDelayValue));
        assertThat(profileSettings.tcpKeepAlive, equalTo(keepAliveValue));
        assertThat(profileSettings.tcpKeepIdle, equalTo(keepIdleValue));
        assertThat(profileSettings.tcpKeepInterval, equalTo(keepIntervalValue));
        assertThat(profileSettings.tcpKeepCount, equalTo(keepCountValue));
        assertThat(profileSettings.reuseAddress, equalTo(reuseAddressValue));
        assertThat(profileSettings.sendBufferSize, equalTo(sendBufferSizeValue));
        assertThat(profileSettings.receiveBufferSize, equalTo(receiveBufferSizeValue));
        assertThat(profileSettings.isDefaultProfile, equalTo(false));
    }

}
