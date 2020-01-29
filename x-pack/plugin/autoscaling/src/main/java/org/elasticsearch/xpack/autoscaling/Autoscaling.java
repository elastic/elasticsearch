/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

/**
 * Container class for autoscaling functionality.
 */
public class Autoscaling extends Plugin {

    public static final Setting<Boolean> AUTOSCALING_ENABLED_SETTING = Setting.boolSetting(
        "xpack.autoscaling.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * The settings defined by autoscaling.
     *
     * @return the settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        if (isSnapshot()) {
            return List.of(AUTOSCALING_ENABLED_SETTING);
        } else {
            return List.of();
        }
    }

    boolean isSnapshot() {
        return Build.CURRENT.isSnapshot();
    }

}
