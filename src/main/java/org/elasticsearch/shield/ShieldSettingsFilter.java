/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;

/**
 *
 */
public class ShieldSettingsFilter {

    static final String HIDE_SETTINGS_SETTING = "shield.hide_settings";

    private final SettingsFilter filter;

    @Inject
    public ShieldSettingsFilter(Settings settings, SettingsFilter settingsFilter) {
        this.filter = settingsFilter;
        filter.addFilter(HIDE_SETTINGS_SETTING);
        filterOut(settings.getAsArray(HIDE_SETTINGS_SETTING));
    }

    public void filterOut(String... patterns) {
        for (String pattern : patterns) {
            filter.addFilter(pattern);
        }
    }
}
