/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
