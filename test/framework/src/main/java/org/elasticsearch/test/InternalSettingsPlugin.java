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
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.plugins.Plugin;

import java.util.Arrays;
import java.util.List;

public final class InternalSettingsPlugin extends Plugin {

    public static final Setting<Integer> VERSION_CREATED =
        Setting.intSetting("index.version.created", 0, Property.IndexScope, Property.NodeScope);
    public static final Setting<Boolean> MERGE_ENABLED =
        Setting.boolSetting("index.merge.enabled", true, Property.IndexScope, Property.NodeScope);
    public static final Setting<Long> INDEX_CREATION_DATE_SETTING =
        Setting.longSetting(IndexMetaData.SETTING_CREATION_DATE, -1, -1, Property.IndexScope, Property.NodeScope);

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(VERSION_CREATED, MERGE_ENABLED, INDEX_CREATION_DATE_SETTING);
    }
}
