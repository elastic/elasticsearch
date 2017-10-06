/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

import static org.elasticsearch.xpack.ccr.CcrSettings.CCR_ENABLED_SETTING;

/**
 * Container class for CCR functionality.
 */
public final class Ccr {

    @SuppressWarnings("unused,FieldCanBeLocal")
    private final boolean enabled;

    /**
     * Construct an instance of the CCR container with the specified settings.
     *
     * @param settings the settings
     */
    public Ccr(final Settings settings) {
        this.enabled = CCR_ENABLED_SETTING.get(settings);
    }

    /**
     * The settings defined by CCR.
     *
     * @return the settings
     */
    public List<Setting<?>> getSettings() {
        return CcrSettings.getSettings();
    }

}
