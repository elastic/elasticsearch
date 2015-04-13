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

package org.elasticsearch.cluster.settings;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.regex.Regex;

import java.util.Map;

/**
 */
public class DynamicSettings {

    private ImmutableMap<String, Validator> dynamicSettings = ImmutableMap.of();


    public boolean isDynamicOrLoggingSetting(String key) {
        return hasDynamicSetting(key) || key.startsWith("logger.");
    }

    public boolean hasDynamicSetting(String key) {
        for (String dynamicSetting : dynamicSettings.keySet()) {
            if (Regex.simpleMatch(dynamicSetting, key)) {
                return true;
            }
        }
        return false;
    }

    public String validateDynamicSetting(String dynamicSetting, String value) {
        for (Map.Entry<String, Validator> setting : dynamicSettings.entrySet()) {
            if (Regex.simpleMatch(setting.getKey(), dynamicSetting)) {
                return setting.getValue().validate(dynamicSetting, value);
            }
        }
        return null;
    }

    public synchronized void addDynamicSetting(String setting, Validator validator) {
        MapBuilder<String, Validator> updatedSettings = MapBuilder.newMapBuilder(dynamicSettings);
        updatedSettings.put(setting, validator);
        dynamicSettings = updatedSettings.immutableMap();
    }

    public synchronized void addDynamicSetting(String setting) {
        addDynamicSetting(setting, Validator.EMPTY);
    }


    public synchronized void addDynamicSettings(String... settings) {
        MapBuilder<String, Validator> updatedSettings = MapBuilder.newMapBuilder(dynamicSettings);
        for (String setting : settings) {
            updatedSettings.put(setting, Validator.EMPTY);
        }
        dynamicSettings = updatedSettings.immutableMap();
    }

}
