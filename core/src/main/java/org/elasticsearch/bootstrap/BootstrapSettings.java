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

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

public final class BootstrapSettings {

    private BootstrapSettings() {
    }

    // TODO: remove this hack when insecure defaults are removed from java
    public static final Setting<Boolean> SECURITY_FILTER_BAD_DEFAULTS_SETTING =
            Setting.boolSetting("security.manager.filter_bad_defaults", true, Property.NodeScope);

    public static final Setting<Boolean> MEMORY_LOCK_SETTING =
        Setting.boolSetting("bootstrap.memory_lock", false, Property.NodeScope);
    public static final Setting<Boolean> SECCOMP_SETTING =
        Setting.boolSetting("bootstrap.seccomp", true, Property.NodeScope);
    public static final Setting<Boolean> CTRLHANDLER_SETTING =
        Setting.boolSetting("bootstrap.ctrlhandler", true, Property.NodeScope);
    public static final Setting<Boolean> IGNORE_SYSTEM_BOOTSTRAP_CHECKS =
            Setting.boolSetting("bootstrap.ignore_system_bootstrap_checks", false, Property.NodeScope);

}
