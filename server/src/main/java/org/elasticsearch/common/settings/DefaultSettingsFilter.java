/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestRequest;

import java.util.Collection;
import java.util.Set;

/**
 * A class that allows to filter settings objects by simple regular expression patterns or full settings keys.
 * It's used for response filtering on the rest layer to for instance filter out sensitive information like access keys.
 */
public final class DefaultSettingsFilter implements SettingsFilter {
    /**
     * Can be used to specify settings filter that will be used to filter out matching settings in toXContent method
     */
    public static String SETTINGS_FILTER_PARAM = "settings_filter";

    private final Set<String> patterns;
    private final String patternString;
    private final ServerlessOperatorSettingsFilter filter;

    public DefaultSettingsFilter(Collection<String> patterns, ServerlessOperatorSettingsFilter filter) {
        for (String pattern : patterns) {
            if (SettingsFilter.isValidPattern(pattern) == false) {
                throw new IllegalArgumentException("invalid pattern: " + pattern);
            }
        }
        this.patterns = Set.copyOf(patterns);
        patternString = Strings.collectionToDelimitedString(patterns, ",");
        this.filter = filter;
    }

    public DefaultSettingsFilter(Collection<String> patterns) {
        this(patterns, null);
    }

    /**
     * Returns a set of patterns
     */
    public Set<String> getPatterns() {
        return patterns;
    }

    @Override
    public void addFilterSettingParams(RestRequest request) {
        if (patterns.isEmpty() == false) {
            request.params().put(SETTINGS_FILTER_PARAM, patternString);
        }
    }

    @Override
    public Settings filter(Settings settings) {
        return SettingsFilter.filterSettings(patterns, settings);
    }

    @Override
    public void validateSettings(Settings settings) {
        // do nothing in es main
    }
}
