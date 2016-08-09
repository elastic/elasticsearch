/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

/**
 * A container for xpack setting constants.
 */
public class XPackSettings {
    /** All setting constants created in this class. */
    private static final List<Setting<?>> ALL_SETTINGS = new ArrayList<>();

    /** Setting for enabling or disabling security. Defaults to true. */
    public static final Setting<Boolean> SECURITY_ENABLED = enabledSetting(XPackPlugin.SECURITY, true);

    /** Setting for enabling or disabling monitoring. Defaults to true if not a tribe node. */
    public static final Setting<Boolean> MONITORING_ENABLED = enabledSetting(XPackPlugin.MONITORING,
        // By default, monitoring is disabled on tribe nodes
        s -> String.valueOf(XPackPlugin.isTribeNode(s) == false && XPackPlugin.isTribeClientNode(s) == false));

    /** Setting for enabling or disabling watcher. Defaults to true. */
    public static final Setting<Boolean> WATCHER_ENABLED = enabledSetting(XPackPlugin.WATCHER, true);

    /** Setting for enabling or disabling graph. Defaults to true. */
    public static final Setting<Boolean> GRAPH_ENABLED = enabledSetting(XPackPlugin.GRAPH, true);

    /** Setting for enabling or disabling auditing. Defaults to false. */
    public static final Setting<Boolean> AUDIT_ENABLED = enabledSetting(XPackPlugin.SECURITY + ".audit", false);

    /** Setting for enabling or disabling document/field level security. Defaults to true. */
    public static final Setting<Boolean> DLS_FLS_ENABLED = enabledSetting(XPackPlugin.SECURITY + ".dls_fls", true);

    /**
     * Create a Setting for the enabled state of features in xpack.
     *
     * The given feature by be enabled or disabled with:
     * {@code "xpack.<feature>.enabled": true | false}
     *
     * For backward compatibility with 1.x and 2.x, the following also works:
     * {@code "<feature>.enabled": true | false}
     *
     * @param featureName The name of the feature in xpack
     * @param defaultValue True if the feature should be enabled by defualt, false otherwise
     */
    private static Setting<Boolean> enabledSetting(String featureName, boolean defaultValue) {
        return enabledSetting(featureName, s -> String.valueOf(defaultValue));
    }

    /**
     * Create a setting for the enabled state of a feature, with a complex default value.
     * @param featureName The name of the feature in xpack
     * @param defaultValueFn A function to determine the default value based on the existing settings
     * @see #enabledSetting(String,boolean)
     */
    private static Setting<Boolean> enabledSetting(String featureName, Function<Settings,String> defaultValueFn) {
        String fallbackName = featureName + ".enabled";
        Setting<Boolean> fallback = Setting.boolSetting(fallbackName, defaultValueFn,
            Setting.Property.NodeScope, Setting.Property.Deprecated);
        String settingName = XPackPlugin.featureSettingPrefix(featureName) + ".enabled";
        Setting<Boolean> setting = Setting.boolSetting(settingName, fallback, Setting.Property.NodeScope);
        ALL_SETTINGS.add(setting);
        return setting;
    }

    /** Returns all settings created in {@link XPackSettings}. */
    static List<Setting<?>> getAllSettings() {
        return Collections.unmodifiableList(ALL_SETTINGS);
    }
}
