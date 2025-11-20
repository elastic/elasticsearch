/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.common.settings.Setting;

import java.util.List;

public class CCMSettings {
    public static final Setting<Boolean> CCM_SUPPORTED_ENVIRONMENT = Setting.boolSetting(
        "xpack.inference.elastic.ccm_supported_environment",
        false,
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(CCM_SUPPORTED_ENVIRONMENT);
    }
}
