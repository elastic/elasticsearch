/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 *
 */
public class ShieldSettingsFilter extends AbstractComponent implements SettingsFilter.Filter {

    static final String HIDE_SETTINGS_SETTING = "shield.hide_settings";

    private final Set<String> removePatterns;

    @Inject
    public ShieldSettingsFilter(Settings settings, SettingsFilter settingsFilter) {
        super(settings);
        settingsFilter.addFilter(this);
        this.removePatterns = new CopyOnWriteArraySet<>();
        removePatterns.add(HIDE_SETTINGS_SETTING);
        Collections.addAll(removePatterns, settings.getAsArray(HIDE_SETTINGS_SETTING));
    }

    public void filterOut(String... patterns) {
        Collections.addAll(removePatterns, patterns);
    }

    @Override
    public void filter(ImmutableSettings.Builder settings) {
        for (Iterator<Map.Entry<String, String>> iter = settings.internalMap().entrySet().iterator(); iter.hasNext();) {
            Map.Entry<String, String> setting = iter.next();
            for (String regexp : removePatterns) {
                if (Regex.simpleMatch(regexp, setting.getKey())) {
                    iter.remove();
                }
            }
        }
    }
}
