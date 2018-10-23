/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Arrays;
import java.util.List;

/**
 * Container class for CCR settings.
 */
public final class CcrSettings {

    // prevent construction
    private CcrSettings() {

    }

    /**
     * Index setting for a following index.
     */
    public static final Setting<Boolean> CCR_FOLLOWING_INDEX_SETTING =
            Setting.boolSetting("index.xpack.ccr.following_index", false, Property.IndexScope, Property.InternalIndex);

    /**
     * Setting for controlling the interval in between polling leader clusters to check whether there are indices to follow
     */
    public static final Setting<TimeValue> CCR_AUTO_FOLLOW_POLL_INTERVAL =
        Setting.timeSetting("xpack.ccr.auto_follow.poll_interval", TimeValue.timeValueMillis(2500), Property.NodeScope);

    /**
     * The settings defined by CCR.
     *
     * @return the settings
     */
    static List<Setting<?>> getSettings() {
        return Arrays.asList(
                XPackSettings.CCR_ENABLED_SETTING,
                CCR_FOLLOWING_INDEX_SETTING,
                CCR_AUTO_FOLLOW_POLL_INTERVAL);
    }

}
