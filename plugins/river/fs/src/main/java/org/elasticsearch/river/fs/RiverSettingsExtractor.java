/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.river.fs;

import org.elasticsearch.river.RiverSettings;

import java.util.Collections;
import java.util.Map;

public class RiverSettingsExtractor {

    public static Map<String, Object> getSettingsGroup(RiverSettings settings, String groupName) {
        return getSettingsGroup(settings.settings(), groupName);
    }

    @SuppressWarnings({"unchecked"})
    public static Map<String, Object> getSettingsGroup(Map<String, Object> settings, String groupName) {
        if (!settings.containsKey(groupName)) {
            return Collections.emptyMap();
        }
        return (Map<String, Object>) settings.get(groupName);
    }
}