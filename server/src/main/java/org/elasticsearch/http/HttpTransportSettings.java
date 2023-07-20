/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.settings.Setting.listSetting;
import static org.elasticsearch.common.settings.Setting.stringListSetting;

public final class HttpTransportSettings {

    public static final Setting<Boolean> SETTING_CORS_ENABLED = Setting.boolSetting("http.cors.enabled", false, Property.NodeScope);
    public static final Setting<String> SETTING_CORS_ALLOW_ORIGIN = new Setting<>(
        "http.cors.allow-origin",
        "",
        (value) -> value,
        Property.NodeScope
    );
    public static final Setting<Integer> SETTING_CORS_MAX_AGE = intSetting("http.cors.max-age", 1728000, Property.NodeScope);
    public static final Setting<String> SETTING_CORS_ALLOW_METHODS = new Setting<>(
        "http.cors.allow-methods",
        "OPTIONS,HEAD,GET,POST,PUT,DELETE",
        (value) -> value,
        Property.NodeScope
    );
    public static final Setting<String> SETTING_CORS_ALLOW_HEADERS = new Setting<>(
        "http.cors.allow-headers",
        "X-Requested-With,Content-Type,Content-Length,Authorization,Accept,User-Agent,X-Elastic-Client-Meta",
        (value) -> value,
        Property.NodeScope
    );
    public static final Setting<String> SETTING_CORS_EXPOSE_HEADERS = new Setting<>(
        "http.cors.expose-headers",
        "X-elastic-product",
        (value) -> value,
        Property.NodeScope
    );
    public static final Setting<Boolean> SETTING_CORS_ALLOW_CREDENTIALS = Setting.boolSetting(
        "http.cors.allow-credentials",
        false,
        Property.NodeScope
    );
    public static final Setting<Integer> SETTING_PIPELINING_MAX_EVENTS = intSetting(
        "http.pipelining.max_events",
        10000,
        Property.NodeScope
    );
    public static final Setting<Boolean> SETTING_HTTP_COMPRESSION = Setting.boolSetting("http.compression", true, Property.NodeScope);
    // we intentionally use a different compression level as Netty here as our benchmarks have shown that a compression level of 3 is the
    // best compromise between reduction in network traffic and added latency. For more details please check #7309.
    public static final Setting<Integer> SETTING_HTTP_COMPRESSION_LEVEL = intSetting("http.compression_level", 3, Property.NodeScope);
    public static final Setting<List<String>> SETTING_HTTP_HOST = stringListSetting("http.host", Property.NodeScope);
    public static final Setting<List<String>> SETTING_HTTP_PUBLISH_HOST = listSetting(
        "http.publish_host",
        SETTING_HTTP_HOST,
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<List<String>> SETTING_HTTP_BIND_HOST = listSetting(
        "http.bind_host",
        SETTING_HTTP_HOST,
        Function.identity(),
        Property.NodeScope
    );

    public static final Setting<PortsRange> SETTING_HTTP_PORT = new Setting<>(
        "http.port",
        "9200-9300",
        PortsRange::new,
        Property.NodeScope
    );
    public static final Setting<Integer> SETTING_HTTP_PUBLISH_PORT = intSetting("http.publish_port", -1, -1, Property.NodeScope);
    public static final Setting<Boolean> SETTING_HTTP_DETAILED_ERRORS_ENABLED = Setting.boolSetting(
        "http.detailed_errors.enabled",
        true,
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_CONTENT_LENGTH = Setting.byteSizeSetting(
        "http.max_content_length",
        new ByteSizeValue(100, ByteSizeUnit.MB),
        ByteSizeValue.ZERO,
        ByteSizeValue.ofBytes(Integer.MAX_VALUE),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_CHUNK_SIZE = Setting.byteSizeSetting(
        "http.max_chunk_size",
        new ByteSizeValue(8, ByteSizeUnit.KB),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_HEADER_SIZE = Setting.byteSizeSetting(
        "http.max_header_size",
        new ByteSizeValue(16, ByteSizeUnit.KB),
        Property.NodeScope
    );
    public static final Setting<Integer> SETTING_HTTP_MAX_WARNING_HEADER_COUNT = intSetting(
        "http.max_warning_header_count",
        -1,
        -1,
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_WARNING_HEADER_SIZE = Setting.byteSizeSetting(
        "http.max_warning_header_size",
        ByteSizeValue.MINUS_ONE,
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_INITIAL_LINE_LENGTH = Setting.byteSizeSetting(
        "http.max_initial_line_length",
        new ByteSizeValue(4, ByteSizeUnit.KB),
        Property.NodeScope
    );

    public static final Setting<TimeValue> SETTING_HTTP_SERVER_SHUTDOWN_GRACE_PERIOD = Setting.positiveTimeSetting(
        "http.shutdown_grace_period",
        TimeValue.timeValueMillis(0),
        Setting.Property.NodeScope
    );

    // don't reset cookies by default, since I don't think we really need to
    // note, parsing cookies was fixed in netty 3.5.1 regarding stack allocation, but still, currently, we don't need cookies
    public static final Setting<Boolean> SETTING_HTTP_RESET_COOKIES = Setting.boolSetting("http.reset_cookies", false, Property.NodeScope);

    // A default of 0 means that by default there is no read timeout
    public static final Setting<TimeValue> SETTING_HTTP_READ_TIMEOUT = Setting.timeSetting(
        "http.read_timeout",
        new TimeValue(0),
        new TimeValue(0),
        Property.NodeScope
    );

    // Tcp socket settings

    public static final Setting<Boolean> SETTING_HTTP_TCP_NO_DELAY = boolSetting(
        "http.tcp.no_delay",
        NetworkService.TCP_NO_DELAY,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> SETTING_HTTP_TCP_KEEP_ALIVE = boolSetting(
        "http.tcp.keep_alive",
        NetworkService.TCP_KEEP_ALIVE,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> SETTING_HTTP_TCP_KEEP_IDLE = intSetting(
        "http.tcp.keep_idle",
        NetworkService.TCP_KEEP_IDLE,
        -1,
        300,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> SETTING_HTTP_TCP_KEEP_INTERVAL = intSetting(
        "http.tcp.keep_interval",
        NetworkService.TCP_KEEP_INTERVAL,
        -1,
        300,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> SETTING_HTTP_TCP_KEEP_COUNT = intSetting(
        "http.tcp.keep_count",
        NetworkService.TCP_KEEP_COUNT,
        -1,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> SETTING_HTTP_TCP_REUSE_ADDRESS = boolSetting(
        "http.tcp.reuse_address",
        NetworkService.TCP_REUSE_ADDRESS,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_TCP_SEND_BUFFER_SIZE = Setting.byteSizeSetting(
        "http.tcp.send_buffer_size",
        NetworkService.TCP_SEND_BUFFER_SIZE,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE = Setting.byteSizeSetting(
        "http.tcp.receive_buffer_size",
        NetworkService.TCP_RECEIVE_BUFFER_SIZE,
        Setting.Property.NodeScope
    );

    public static final Setting<List<String>> SETTING_HTTP_TRACE_LOG_INCLUDE = Setting.stringListSetting(
        "http.tracer.include",
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<List<String>> SETTING_HTTP_TRACE_LOG_EXCLUDE = Setting.stringListSetting(
        "http.tracer.exclude",
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> SETTING_HTTP_CLIENT_STATS_ENABLED = boolSetting(
        "http.client_stats.enabled",
        true,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Integer> SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_COUNT = intSetting(
        "http.client_stats.closed_channels.max_count",
        10000,
        Property.NodeScope
    );
    public static final Setting<TimeValue> SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_AGE = Setting.timeSetting(
        "http.client_stats.closed_channels.max_age",
        TimeValue.timeValueMinutes(5),
        Property.NodeScope
    );

    private HttpTransportSettings() {}
}
