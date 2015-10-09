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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.regex.Regex;

/**
 * A container for setting names and validation methods for those settings.
 */
public class DynamicSettings {
    private final ImmutableOpenMap<String, Validator> dynamicSettings;

    public static class Builder {
        private ImmutableOpenMap.Builder<String, Validator> settings = ImmutableOpenMap.builder();

        public void addSetting(String setting, Validator validator) {
            Validator old = settings.put(setting, validator);
            if (old != null) {
                throw new IllegalArgumentException("Cannot register setting [" + setting + "] twice");
            }
        }

        public DynamicSettings build() {
            return new DynamicSettings(settings.build());
        }
    }

    private DynamicSettings(ImmutableOpenMap<String, Validator> settings) {
        this.dynamicSettings = settings;
    }

    public boolean isDynamicOrLoggingSetting(String key) {
        return hasDynamicSetting(key) || key.startsWith("logger.");
    }

    public boolean hasDynamicSetting(String key) {
        for (ObjectCursor<String> dynamicSetting : dynamicSettings.keys()) {
            if (Regex.simpleMatch(dynamicSetting.value, key)) {
                return true;
            }
        }
        return false;
    }

    public String validateDynamicSetting(String dynamicSetting, String value, ClusterState clusterState) {
        for (ObjectObjectCursor<String, Validator> setting : dynamicSettings) {
            if (Regex.simpleMatch(setting.key, dynamicSetting)) {
                return setting.value.validate(dynamicSetting, value, clusterState);
            }
        }
        return null;
    }
}
