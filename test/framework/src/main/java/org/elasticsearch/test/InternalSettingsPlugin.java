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
package org.elasticsearch.test;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.plugins.Plugin;

public final class InternalSettingsPlugin extends Plugin {
    @Override
    public String name() {
        return "internal-settings-plugin";
    }

    @Override
    public String description() {
        return "a plugin that allows to set values for internal settings which are can't be set via the ordinary API without this pluging installed";
    }

    public static final Setting<Integer> VERSION_CREATED = Setting.intSetting("index.version.created", 0, false, Setting.Scope.INDEX);
    public static final Setting<Boolean> MERGE_ENABLED = Setting.boolSetting("index.merge.enabled", true, false, Setting.Scope.INDEX);
    public static final Setting<Long> INDEX_CREATION_DATE_SETTING = Setting.longSetting(IndexMetaData.SETTING_CREATION_DATE, -1, -1, false, Setting.Scope.INDEX);

    public void onModule(SettingsModule module) {
        module.registerSetting(VERSION_CREATED);
        module.registerSetting(MERGE_ENABLED);
        module.registerSetting(INDEX_CREATION_DATE_SETTING);
    }
}
