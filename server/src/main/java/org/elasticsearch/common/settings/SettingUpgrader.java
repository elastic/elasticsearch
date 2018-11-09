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

package org.elasticsearch.common.settings;

import java.util.List;

/**
 * Represents the logic to upgrade a setting.
 *
 * @param <T> the type of the underlying setting
 */
public interface SettingUpgrader<T> {

    /**
     * The setting upgraded by this upgrader.
     *
     * @return the setting
     */
    Setting<T> getSetting();

    /**
     * The logic to upgrade the setting key, for example by mapping the old setting key to the new setting key.
     *
     * @param key the setting key to upgrade
     * @return the upgraded setting key
     */
    String getKey(String key);

    /**
     * The logic to upgrade the setting value.
     *
     * @param value the setting value to upgrade
     * @return the upgraded setting value
     */
    default String getValue(final String value) {
        return value;
    }

    default List<String> getListValue(final List<String> value) {
        return value;
    }

}
