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

package org.elasticsearch.river;

import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 * (shayy.banon)
 */

public class RiverSettings {

    private final Settings globalSettings;

    private final Map<String, Object> settings;

    public RiverSettings(Settings globalSettings, Map<String, Object> settings) {
        this.globalSettings = globalSettings;
        this.settings = settings;
    }

    public Settings globalSettings() {
        return globalSettings;
    }

    public Map<String, Object> settings() {
        return settings;
    }
}
