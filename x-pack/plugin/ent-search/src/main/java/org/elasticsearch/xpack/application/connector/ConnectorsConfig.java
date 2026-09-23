/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Strings;

import java.util.List;

public class ConnectorsConfig {

    static final String SETTING_ROOT_PATH = "xpack.applications.connector";

    private static final int DEFAULT_DESCRIPTION_LENGTH = 8192;
    private static final int MIN_DESCRIPTION_LENGTH = 1;
    private static final int MAX_DESCRIPTION_LENGTH = 65536;

    /**
     * Bounds the heap needed to read a {@link Connector}, as the description is stored and returned in full.
     */
    public static final Setting<Integer> MAX_DESCRIPTION_LENGTH_SETTING = Setting.intSetting(
        Strings.format("%s.%s", SETTING_ROOT_PATH, "max_description_length"),
        DEFAULT_DESCRIPTION_LENGTH,
        MIN_DESCRIPTION_LENGTH,
        MAX_DESCRIPTION_LENGTH,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> getSettings() {
        return List.of(MAX_DESCRIPTION_LENGTH_SETTING);
    }

    private ConnectorsConfig() {}

}
