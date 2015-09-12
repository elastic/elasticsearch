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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.regex.Regex;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A container for setting names and validation methods for those settings.
 */
public class DynamicSettings {

    private final Map<String, Validator> dynamicSettings;

    public static class Builder {
        private Map<String, Validator> settings = new HashMap<>();

        public void addSetting(String setting, Validator validator) {
            Validator old = settings.put(setting, validator);
            if (old != null) {
                throw new IllegalArgumentException("Cannot register setting [" + setting + "] twice");
            }
        }

        public DynamicSettings build() {
            return new DynamicSettings(settings);
        }
    }

    private DynamicSettings(Map<String, Validator> settings) {
        this.dynamicSettings = Collections.unmodifiableMap(settings);
    }

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

    public String validateDynamicSetting(String dynamicSetting, String value, ClusterState clusterState) {
        for (Map.Entry<String, Validator> setting : dynamicSettings.entrySet()) {
            if (Regex.simpleMatch(setting.getKey(), dynamicSetting)) {
                return setting.getValue().validate(dynamicSetting, value, clusterState);
            }
        }
        return null;
    }
}
