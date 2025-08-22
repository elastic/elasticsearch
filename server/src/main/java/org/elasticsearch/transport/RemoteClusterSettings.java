/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.enumSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

public class RemoteClusterSettings {

    /**
     * The initial connect timeout for remote cluster connections
     */
    public static final Setting<TimeValue> REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "cluster.remote.initial_connect_timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );
    /**
     * The name of a node attribute to select nodes that should be connected to in the remote cluster.
     * For instance a node can be configured with {@code node.attr.gateway: true} in order to be eligible as a gateway node between
     * clusters. In that case {@code cluster.remote.node.attr: gateway} can be used to filter out other nodes in the remote cluster.
     * The value of the setting is expected to be a boolean, {@code true} for nodes that can become gateways, {@code false} otherwise.
     */
    public static final Setting<String> REMOTE_NODE_ATTRIBUTE = Setting.simpleString(
        "cluster.remote.node.attr",
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<Boolean> REMOTE_CLUSTER_SKIP_UNAVAILABLE = Setting.affixKeySetting(
        "cluster.remote.",
        "skip_unavailable",
        (ns, key) -> boolSetting(key, true, new RemoteConnectionEnabled<>(ns, key), Setting.Property.Dynamic, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<TimeValue> REMOTE_CLUSTER_PING_SCHEDULE = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.ping_schedule",
        (ns, key) -> timeSetting(
            key,
            TransportSettings.PING_SCHEDULE,
            new RemoteConnectionEnabled<>(ns, key),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );
    public static final Setting.AffixSetting<Compression.Enabled> REMOTE_CLUSTER_COMPRESS = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.compress",
        (ns, key) -> enumSetting(
            Compression.Enabled.class,
            key,
            TransportSettings.TRANSPORT_COMPRESS,
            new RemoteConnectionEnabled<>(ns, key),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );
    public static final Setting.AffixSetting<Compression.Scheme> REMOTE_CLUSTER_COMPRESSION_SCHEME = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.compression_scheme",
        (ns, key) -> enumSetting(
            Compression.Scheme.class,
            key,
            TransportSettings.TRANSPORT_COMPRESSION_SCHEME,
            new RemoteConnectionEnabled<>(ns, key),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );
    public static final Setting.AffixSetting<SecureString> REMOTE_CLUSTER_CREDENTIALS = Setting.affixKeySetting(
        "cluster.remote.",
        "credentials",
        key -> SecureSetting.secureString(key, null)
    );

    private static class RemoteConnectionEnabled<T> implements Setting.Validator<T> {

        private final String clusterAlias;
        private final String key;

        private RemoteConnectionEnabled(String clusterAlias, String key) {
            this.clusterAlias = clusterAlias;
            this.key = key;
        }

        @Override
        public void validate(T value) {}

        @Override
        public void validate(T value, Map<Setting<?>, Object> settings, boolean isPresent) {
            if (isPresent && RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, settings) == false) {
                throw new IllegalArgumentException("Cannot configure setting [" + key + "] if remote cluster is not enabled.");
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return Stream.concat(
                Stream.of(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias)),
                settingsStream()
            ).iterator();
        }

        private Stream<Setting<?>> settingsStream() {
            return Arrays.stream(RemoteConnectionStrategy.ConnectionStrategy.values())
                .flatMap(strategy -> strategy.getEnablementSettings().get())
                .map(as -> as.getConcreteSettingForNamespace(clusterAlias));
        }
    }
}
