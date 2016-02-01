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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * a collection of settings related to transport components, which are also needed in org.elasticsearch.bootstrap.Security
 * This class should only contain static code which is *safe* to load before the security manager is enforced.
 */
final public class TransportSettings {

    public static final Setting<List<String>> HOST = Setting.listSetting("transport.host", emptyList(), s -> s, false, Setting.Scope.CLUSTER);
    public static final Setting<List<String>> PUBLISH_HOST = Setting.listSetting("transport.publish_host", HOST, s -> s, false, Setting.Scope.CLUSTER);
    public static final Setting<List<String>> BIND_HOST = Setting.listSetting("transport.bind_host", HOST, s -> s, false, Setting.Scope.CLUSTER);
    public static final Setting<String> PORT = new Setting<>("transport.tcp.port", "9300-9400", s -> s, false, Setting.Scope.CLUSTER);
    public static final Setting<Integer> PUBLISH_PORT = Setting.intSetting("transport.publish_port", -1, -1, false, Setting.Scope.CLUSTER);
    public static final String DEFAULT_PROFILE = "default";
    public static final Setting<Settings> TRANSPORT_PROFILES_SETTING = Setting.groupSetting("transport.profiles.", true, Setting.Scope.CLUSTER);

    private TransportSettings() {

    }
}
