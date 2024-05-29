/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.ToXContent.Params;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A class that allows to filter settings objects by simple regular expression patterns or full settings keys.
 * It's used for response filtering on the rest layer to for instance filter out sensitive information like access keys.
 */
public final class SettingsFilter {
    /**
     * Can be used to specify settings filter that will be used to filter out matching settings in toXContent method
     */
    public static String SETTINGS_FILTER_PARAM = "settings_filter";

    private final Set<String> patterns;
    private final String patternString;

    public SettingsFilter(Collection<String> patterns) {
        for (String pattern : patterns) {
            if (isValidPattern(pattern) == false) {
                throw new IllegalArgumentException("invalid pattern: " + pattern);
            }
        }
        this.patterns = Set.copyOf(patterns);
        patternString = Strings.collectionToDelimitedString(patterns, ",");
    }

    /**
     * Returns a set of patterns
     */
    public Set<String> getPatterns() {
        return patterns;
    }

    /**
     * Returns <code>true</code> iff the given string is either a valid settings key pattern or a simple regular expression
     * @see Regex
     * @see AbstractScopedSettings#isValidKey(String)
     */
    public static boolean isValidPattern(String pattern) {
        return AbstractScopedSettings.isValidKey(pattern) || Regex.isSimpleMatchPattern(pattern);
    }

    public void addFilterSettingParams(RestRequest request) {
        if (patterns.isEmpty() == false) {
            request.params().put(SETTINGS_FILTER_PARAM, patternString);
        }
    }

    public static Settings filterSettings(Params params, Settings settings) {
        String patterns = params.param(SETTINGS_FILTER_PARAM);
        final Settings filteredSettings;
        if (patterns != null && patterns.isEmpty() == false) {
            filteredSettings = filterSettings(Strings.commaDelimitedListToSet(patterns), settings);
        } else {
            filteredSettings = settings;
        }
        return filteredSettings;
    }

    public Settings filter(Settings settings) {
        return filterSettings(patterns, settings);
    }

    private static Settings filterSettings(Iterable<String> patterns, Settings settings) {
        Settings.Builder builder = Settings.builder().put(settings);
        List<String> simpleMatchPatternList = new ArrayList<>();
        for (String pattern : patterns) {
            if (Regex.isSimpleMatchPattern(pattern)) {
                simpleMatchPatternList.add(pattern);
            } else {
                builder.remove(pattern);
            }
        }
        if (simpleMatchPatternList.isEmpty() == false) {
            String[] simpleMatchPatterns = simpleMatchPatternList.toArray(String[]::new);
            builder.keys().removeIf(key -> Regex.simpleMatch(simpleMatchPatterns, key));
        }
        return builder.build();
    }
}
