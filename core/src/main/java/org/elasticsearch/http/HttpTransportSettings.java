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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Scope;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.settings.Setting.listSetting;

public final class HttpTransportSettings {

    public static final Setting<Boolean> SETTING_CORS_ENABLED = Setting.boolSetting("http.cors.enabled", false, false, Scope.CLUSTER);
    public static final Setting<String> SETTING_CORS_ALLOW_ORIGIN = new Setting<String>("http.cors.allow-origin", "", (value) -> value, false, Scope.CLUSTER);
    public static final Setting<Integer> SETTING_CORS_MAX_AGE = Setting.intSetting("http.cors.max-age", 1728000, false, Scope.CLUSTER);
    public static final Setting<String> SETTING_CORS_ALLOW_METHODS = new Setting<String>("http.cors.allow-methods", "OPTIONS, HEAD, GET, POST, PUT, DELETE", (value) -> value, false, Scope.CLUSTER);
    public static final Setting<String> SETTING_CORS_ALLOW_HEADERS = new Setting<String>("http.cors.allow-headers", "X-Requested-With, Content-Type, Content-Length", (value) -> value, false, Scope.CLUSTER);
    public static final Setting<Boolean> SETTING_CORS_ALLOW_CREDENTIALS = Setting.boolSetting("http.cors.allow-credentials", false, false, Scope.CLUSTER);
    public static final Setting<Boolean> SETTING_PIPELINING = Setting.boolSetting("http.pipelining", true, false, Scope.CLUSTER);
    public static final Setting<Integer> SETTING_PIPELINING_MAX_EVENTS = Setting.intSetting("http.pipelining.max_events", 10000, false, Scope.CLUSTER);
    public static final Setting<Boolean> SETTING_HTTP_COMPRESSION = Setting.boolSetting("http.compression", false, false, Scope.CLUSTER);
    public static final Setting<Integer> SETTING_HTTP_COMPRESSION_LEVEL = Setting.intSetting("http.compression_level", 6, false, Scope.CLUSTER);
    public static final Setting<List<String>> SETTING_HTTP_HOST = listSetting("http.host", emptyList(), s -> s, false, Scope.CLUSTER);
    public static final Setting<List<String>> SETTING_HTTP_PUBLISH_HOST = listSetting("http.publish_host", SETTING_HTTP_HOST, s -> s, false, Scope.CLUSTER);
    public static final Setting<List<String>> SETTING_HTTP_BIND_HOST = listSetting("http.bind_host", SETTING_HTTP_HOST, s -> s, false, Scope.CLUSTER);

    public static final Setting<PortsRange> SETTING_HTTP_PORT = new Setting<PortsRange>("http.port", "9200-9300", PortsRange::new, false, Scope.CLUSTER);
    public static final Setting<Integer> SETTING_HTTP_PUBLISH_PORT = Setting.intSetting("http.publish_port", -1, -1, false, Scope.CLUSTER);
    public static final Setting<Boolean> SETTING_HTTP_DETAILED_ERRORS_ENABLED = Setting.boolSetting("http.detailed_errors.enabled", true, false, Scope.CLUSTER);
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_CONTENT_LENGTH = Setting.byteSizeSetting("http.max_content_length", new ByteSizeValue(100, ByteSizeUnit.MB), false, Scope.CLUSTER) ;
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_CHUNK_SIZE = Setting.byteSizeSetting("http.max_chunk_size", new ByteSizeValue(8, ByteSizeUnit.KB), false, Scope.CLUSTER) ;
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_HEADER_SIZE = Setting.byteSizeSetting("http.max_header_size", new ByteSizeValue(8, ByteSizeUnit.KB), false, Scope.CLUSTER) ;
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_INITIAL_LINE_LENGTH = Setting.byteSizeSetting("http.max_initial_line_length", new ByteSizeValue(4, ByteSizeUnit.KB), false, Scope.CLUSTER) ;
    // don't reset cookies by default, since I don't think we really need to
    // note, parsing cookies was fixed in netty 3.5.1 regarding stack allocation, but still, currently, we don't need cookies
    public static final Setting<Boolean> SETTING_HTTP_RESET_COOKIES = Setting.boolSetting("http.reset_cookies", false, false, Scope.CLUSTER);

    private HttpTransportSettings() {
    }
}
