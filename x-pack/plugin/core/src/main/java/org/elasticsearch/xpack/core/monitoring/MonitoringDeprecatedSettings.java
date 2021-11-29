/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.monitoring;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.TimeValue;

import java.util.Arrays;
import java.util.List;

/**
 * A collection of settings that are marked as deprecated and soon to be removed. These settings have been moved here because the features
 * that make use of them have been removed from the code. Their removals can be enacted after the standard deprecation period has completed.
 */
public final class MonitoringDeprecatedSettings {
    private MonitoringDeprecatedSettings() {}

    // ===================
    // Deprecated in 7.16:
    public static final Setting.AffixSetting<Boolean> TEMPLATE_CREATE_LEGACY_VERSIONS_SETTING = Setting.affixKeySetting(
        "xpack.monitoring.exporters.",
        "index.template.create_legacy_templates",
        (key) -> Setting.boolSetting(key, true, Property.Dynamic, Property.NodeScope, Property.DeprecatedWarning)
    );
    public static final Setting.AffixSetting<Boolean> USE_INGEST_PIPELINE_SETTING = Setting.affixKeySetting(
        "xpack.monitoring.exporters.",
        "use_ingest",
        key -> Setting.boolSetting(key, true, Property.Dynamic, Property.NodeScope, Property.DeprecatedWarning)
    );
    public static final Setting.AffixSetting<TimeValue> PIPELINE_CHECK_TIMEOUT_SETTING = Setting.affixKeySetting(
        "xpack.monitoring.exporters.",
        "index.pipeline.master_timeout",
        (key) -> Setting.timeSetting(key, TimeValue.MINUS_ONE, Property.Dynamic, Property.NodeScope, Property.DeprecatedWarning)
    );
    // ===================

    public static List<Setting.AffixSetting<?>> getSettings() {
        return Arrays.asList(TEMPLATE_CREATE_LEGACY_VERSIONS_SETTING, USE_INGEST_PIPELINE_SETTING, PIPELINE_CHECK_TIMEOUT_SETTING);
    }

}
