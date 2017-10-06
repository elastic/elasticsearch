/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

import java.util.Collections;
import java.util.List;

/**
 * Container class for CCR settings.
 */
final class CcrSettings {

    // prevent construction
    private CcrSettings() {

    }

    /**
     * Setting for controlling whether or not CCR is enabled.
     */
    static final Setting<Boolean> CCR_ENABLED_SETTING = Setting.boolSetting("xpack.ccr.enabled", true, Property.NodeScope);

    static List<Setting<?>> getSettings() {
        return Collections.singletonList(CCR_ENABLED_SETTING);
    }

}
