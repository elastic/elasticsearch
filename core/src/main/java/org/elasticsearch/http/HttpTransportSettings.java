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

package org.elasticsearch.http;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.listSetting;

public final class HttpTransportSettings {

    public static final Setting<Boolean> SETTING_CORS_ENABLED =
        Setting.boolSetting("http.cors.enabled", false, Property.NodeScope);
    public static final Setting<String> SETTING_CORS_ALLOW_ORIGIN =
        new Setting<>("http.cors.allow-origin", "", (value) -> value, Property.NodeScope);
    public static final Setting<Integer> SETTING_CORS_MAX_AGE =
        Setting.intSetting("http.cors.max-age", 1728000, Property.NodeScope);
    public static final Setting<String> SETTING_CORS_ALLOW_METHODS =
        new Setting<>("http.cors.allow-methods", "OPTIONS,HEAD,GET,POST,PUT,DELETE", (value) -> value, Property.NodeScope);
    public static final Setting<String> SETTING_CORS_ALLOW_HEADERS =
        new Setting<>("http.cors.allow-headers", "X-Requested-With,Content-Type,Content-Length", (value) -> value, Property.NodeScope);
    public static final Setting<Boolean> SETTING_CORS_ALLOW_CREDENTIALS =
        Setting.boolSetting("http.cors.allow-credentials", false, Property.NodeScope);
    public static final Setting<Boolean> SETTING_PIPELINING =
        Setting.boolSetting("http.pipelining", true, Property.NodeScope);
    public static final Setting<Integer> SETTING_PIPELINING_MAX_EVENTS =
        Setting.intSetting("http.pipelining.max_events", 10000, Property.NodeScope);
    public static final Setting<Boolean> SETTING_HTTP_COMPRESSION =
        Setting.boolSetting("http.compression", true, Property.NodeScope);
    // we intentionally use a different compression level as Netty here as our benchmarks have shown that a compression level of 3 is the
    // best compromise between reduction in network traffic and added latency. For more details please check #7309.
    public static final Setting<Integer> SETTING_HTTP_COMPRESSION_LEVEL =
        Setting.intSetting("http.compression_level", 3, Property.NodeScope);
    public static final Setting<List<String>> SETTING_HTTP_HOST =
        listSetting("http.host", emptyList(), Function.identity(), Property.NodeScope);
    public static final Setting<List<String>> SETTING_HTTP_PUBLISH_HOST =
        listSetting("http.publish_host", SETTING_HTTP_HOST, Function.identity(), Property.NodeScope);
    public static final Setting<List<String>> SETTING_HTTP_BIND_HOST =
        listSetting("http.bind_host", SETTING_HTTP_HOST, Function.identity(), Property.NodeScope);

    public static final Setting<PortsRange> SETTING_HTTP_PORT =
        new Setting<>("http.port", "9200-9300", PortsRange::new, Property.NodeScope);
    public static final Setting<Integer> SETTING_HTTP_PUBLISH_PORT =
        Setting.intSetting("http.publish_port", -1, -1, Property.NodeScope);
    public static final Setting<Boolean> SETTING_HTTP_DETAILED_ERRORS_ENABLED =
        Setting.boolSetting("http.detailed_errors.enabled", true, Property.NodeScope);
    public static final Setting<Boolean> SETTING_HTTP_CONTENT_TYPE_REQUIRED =
        new Setting<>("http.content_type.required", (s) -> Boolean.toString(true), (s) -> {
            final boolean value = Booleans.parseBoolean(s);
            if (value == false) {
                throw new IllegalArgumentException("http.content_type.required cannot be set to false. It exists only to make a rolling" +
                    " upgrade easier");
            }
            return true;
        }, Property.NodeScope, Property.Deprecated);
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_CONTENT_LENGTH =
        Setting.byteSizeSetting("http.max_content_length", new ByteSizeValue(100, ByteSizeUnit.MB), Property.NodeScope);
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_CHUNK_SIZE =
        Setting.byteSizeSetting("http.max_chunk_size", new ByteSizeValue(8, ByteSizeUnit.KB), Property.NodeScope);
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_HEADER_SIZE =
        Setting.byteSizeSetting("http.max_header_size", new ByteSizeValue(8, ByteSizeUnit.KB), Property.NodeScope);
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_INITIAL_LINE_LENGTH =
        Setting.byteSizeSetting("http.max_initial_line_length", new ByteSizeValue(4, ByteSizeUnit.KB), Property.NodeScope);
    // don't reset cookies by default, since I don't think we really need to
    // note, parsing cookies was fixed in netty 3.5.1 regarding stack allocation, but still, currently, we don't need cookies
    public static final Setting<Boolean> SETTING_HTTP_RESET_COOKIES =
        Setting.boolSetting("http.reset_cookies", false, Property.NodeScope);

    public static final Setting<Boolean> SETTING_HTTP_TCP_NO_DELAY =
        boolSetting("http.tcp_no_delay", NetworkService.TCP_NO_DELAY, Setting.Property.NodeScope);
    public static final Setting<Boolean> SETTING_HTTP_TCP_KEEP_ALIVE =
        boolSetting("http.tcp.keep_alive", NetworkService.TCP_KEEP_ALIVE, Setting.Property.NodeScope);
    public static final Setting<Boolean> SETTING_HTTP_TCP_REUSE_ADDRESS =
        boolSetting("http.tcp.reuse_address", NetworkService.TCP_REUSE_ADDRESS, Setting.Property.NodeScope);
    public static final Setting<ByteSizeValue> SETTING_HTTP_TCP_SEND_BUFFER_SIZE =
        Setting.byteSizeSetting("http.tcp.send_buffer_size", NetworkService.TCP_SEND_BUFFER_SIZE, Setting.Property.NodeScope);
    public static final Setting<ByteSizeValue> SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE =
        Setting.byteSizeSetting("http.tcp.receive_buffer_size", NetworkService.TCP_RECEIVE_BUFFER_SIZE, Setting.Property.NodeScope);

    private HttpTransportSettings() {
    }
}
