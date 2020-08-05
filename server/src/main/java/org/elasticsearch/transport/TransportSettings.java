/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport;

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.settings.Setting.affixKeySetting;
import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.settings.Setting.listSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

public final class TransportSettings {

    public static final String DEFAULT_PROFILE = "default";
    public static final String FEATURE_PREFIX = "transport.features";

    public static final Setting<List<String>> HOST =
        listSetting("transport.host", emptyList(), Function.identity(), Setting.Property.NodeScope);
    public static final Setting<List<String>> PUBLISH_HOST =
        listSetting("transport.publish_host", HOST, Function.identity(), Setting.Property.NodeScope);
    public static final Setting.AffixSetting<List<String>> PUBLISH_HOST_PROFILE =
        affixKeySetting("transport.profiles.", "publish_host", key -> listSetting(key, PUBLISH_HOST, Function.identity(),
            Setting.Property.NodeScope));
    public static final Setting<List<String>> BIND_HOST =
        listSetting("transport.bind_host", HOST, Function.identity(), Setting.Property.NodeScope);
    public static final Setting.AffixSetting<List<String>> BIND_HOST_PROFILE = affixKeySetting("transport.profiles.", "bind_host",
        key -> listSetting(key, BIND_HOST, Function.identity(), Setting.Property.NodeScope));
    public static final Setting<String> PORT =
        new Setting<>("transport.port", "9300-9400", Function.identity(), Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> PORT_PROFILE = affixKeySetting("transport.profiles.", "port",
        key -> new Setting<>(key, PORT, Function.identity(), Setting.Property.NodeScope));
    public static final Setting<Integer> PUBLISH_PORT =
        intSetting("transport.publish_port", -1, -1, Setting.Property.NodeScope);
    public static final Setting.AffixSetting<Integer> PUBLISH_PORT_PROFILE = affixKeySetting("transport.profiles.", "publish_port",
        key -> intSetting(key, -1, -1, Setting.Property.NodeScope));
    public static final Setting<Boolean> TRANSPORT_COMPRESS =
        boolSetting("transport.compress", false, Setting.Property.NodeScope);
    // the scheduled internal ping interval setting, defaults to disabled (-1)
    public static final Setting<TimeValue> PING_SCHEDULE =
        timeSetting("transport.ping_schedule", TimeValue.timeValueSeconds(-1), Setting.Property.NodeScope);
    public static final Setting<TimeValue> CONNECT_TIMEOUT =
        timeSetting("transport.connect_timeout", new TimeValue(30, TimeUnit.SECONDS), Setting.Property.NodeScope);
    public static final Setting<Settings> DEFAULT_FEATURES_SETTING = Setting.groupSetting(FEATURE_PREFIX + ".", Setting.Property.NodeScope);

    // Tcp socket settings

    public static final Setting<Boolean> TCP_NO_DELAY =
        boolSetting("transport.tcp.no_delay", NetworkService.TCP_NO_DELAY, Setting.Property.NodeScope);
    public static final Setting.AffixSetting<Boolean> TCP_NO_DELAY_PROFILE =
        affixKeySetting("transport.profiles.", "tcp.no_delay", key -> boolSetting(key, TCP_NO_DELAY, Setting.Property.NodeScope));
    public static final Setting<Boolean> TCP_KEEP_ALIVE =
        boolSetting("transport.tcp.keep_alive", NetworkService.TCP_KEEP_ALIVE, Setting.Property.NodeScope);
    public static final Setting.AffixSetting<Boolean> TCP_KEEP_ALIVE_PROFILE =
        affixKeySetting("transport.profiles.", "tcp.keep_alive",
            key -> boolSetting(key, TCP_KEEP_ALIVE, Setting.Property.NodeScope));
    public static final Setting<Integer> TCP_KEEP_IDLE =
        intSetting("transport.tcp.keep_idle", NetworkService.TCP_KEEP_IDLE, -1, 300, Setting.Property.NodeScope);
    public static final Setting.AffixSetting<Integer> TCP_KEEP_IDLE_PROFILE =
        affixKeySetting("transport.profiles.", "tcp.keep_idle",
            key -> intSetting(key, TCP_KEEP_IDLE, -1, 300, Setting.Property.NodeScope));
    public static final Setting<Integer> TCP_KEEP_INTERVAL =
        intSetting("transport.tcp.keep_interval", NetworkService.TCP_KEEP_INTERVAL, -1, 300, Setting.Property.NodeScope);
    public static final Setting.AffixSetting<Integer> TCP_KEEP_INTERVAL_PROFILE =
        affixKeySetting("transport.profiles.", "tcp.keep_interval",
            key -> intSetting(key, TCP_KEEP_INTERVAL, -1, 300, Setting.Property.NodeScope));
    public static final Setting<Integer> TCP_KEEP_COUNT =
        intSetting("transport.tcp.keep_count", NetworkService.TCP_KEEP_COUNT, -1, Setting.Property.NodeScope);
    public static final Setting.AffixSetting<Integer> TCP_KEEP_COUNT_PROFILE =
        affixKeySetting("transport.profiles.", "tcp.keep_count",
            key -> intSetting(key, TCP_KEEP_COUNT, -1, Setting.Property.NodeScope));
    public static final Setting<Boolean> TCP_REUSE_ADDRESS =
        boolSetting("transport.tcp.reuse_address", NetworkService.TCP_REUSE_ADDRESS, Setting.Property.NodeScope);
    public static final Setting.AffixSetting<Boolean> TCP_REUSE_ADDRESS_PROFILE =
        affixKeySetting("transport.profiles.", "tcp.reuse_address", key -> boolSetting(key, TCP_REUSE_ADDRESS, Setting.Property.NodeScope));
    public static final Setting<ByteSizeValue> TCP_SEND_BUFFER_SIZE =
        Setting.byteSizeSetting("transport.tcp.send_buffer_size", NetworkService.TCP_SEND_BUFFER_SIZE, Setting.Property.NodeScope);
    public static final Setting.AffixSetting<ByteSizeValue> TCP_SEND_BUFFER_SIZE_PROFILE =
        affixKeySetting("transport.profiles.", "tcp.send_buffer_size",
            key -> Setting.byteSizeSetting(key, TCP_SEND_BUFFER_SIZE, Setting.Property.NodeScope));
    public static final Setting<ByteSizeValue> TCP_RECEIVE_BUFFER_SIZE =
        Setting.byteSizeSetting("transport.tcp.receive_buffer_size", NetworkService.TCP_RECEIVE_BUFFER_SIZE, Setting.Property.NodeScope);
    public static final Setting.AffixSetting<ByteSizeValue> TCP_RECEIVE_BUFFER_SIZE_PROFILE =
        affixKeySetting("transport.profiles.", "tcp.receive_buffer_size",
            key -> Setting.byteSizeSetting(key, TCP_RECEIVE_BUFFER_SIZE, Setting.Property.NodeScope));

    // Connections per node settings

    public static final Setting<Integer> CONNECTIONS_PER_NODE_RECOVERY =
        intSetting("transport.connections_per_node.recovery", 2, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_BULK =
        intSetting("transport.connections_per_node.bulk", 3, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_REG =
        intSetting("transport.connections_per_node.reg", 6, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_STATE =
        intSetting("transport.connections_per_node.state", 1, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_PING =
        intSetting("transport.connections_per_node.ping", 1, 1, Setting.Property.NodeScope);

    // Tracer settings

    public static final Setting<List<String>> TRACE_LOG_INCLUDE_SETTING =
        listSetting("transport.tracer.include", emptyList(), Function.identity(), Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<List<String>> TRACE_LOG_EXCLUDE_SETTING =
        listSetting("transport.tracer.exclude",
            Arrays.asList("internal:coordination/fault_detection/*"),
            Function.identity(), Setting.Property.Dynamic, Setting.Property.NodeScope);

    private TransportSettings() {
    }
}
