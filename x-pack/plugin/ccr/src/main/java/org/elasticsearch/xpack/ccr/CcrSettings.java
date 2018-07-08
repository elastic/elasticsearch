/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

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
     * Setting for controlling whether or not CCR is enabled.
     */
    static final Setting<Boolean> CCR_ENABLED_SETTING = Setting.boolSetting("xpack.ccr.enabled", true, Property.NodeScope);

    /**
     * Index setting for a following index.
     */
    public static final Setting<Boolean> CCR_FOLLOWING_INDEX_SETTING =
            Setting.boolSetting("index.xpack.ccr.following_index", false, Setting.Property.IndexScope);

    /**
     * An internal setting used to keep track to what leader index a follow index associated. Do not use externally!
     */
    public static final Setting<String> CCR_LEADER_INDEX_UUID_SETTING =
            Setting.simpleString("index.xpack.ccr.leader_index_uuid", Setting.Property.IndexScope);

    /**
     * The settings defined by CCR.
     *
     * @return the settings
     */
    static List<Setting<?>> getSettings() {
        return Arrays.asList(
                CCR_ENABLED_SETTING,
                CCR_FOLLOWING_INDEX_SETTING,
                CCR_LEADER_INDEX_UUID_SETTING);
    }

}
