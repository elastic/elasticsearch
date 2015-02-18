/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.settings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.rest.RestRequest;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class SettingsFilter extends AbstractComponent {
    /**
     * Can be used to specify settings filter that will be used to filter out matching settings in toXContent method
     */
    public static String SETTINGS_FILTER_PARAM = "settings_filter";

    private final CopyOnWriteArrayList<String> patterns = new CopyOnWriteArrayList<>();

    @Inject
    public SettingsFilter(Settings settings) {
        super(settings);
    }

    /**
     * Adds a new simple pattern to the list of filters
     *
     * @param pattern
     */
    public void addFilter(String pattern) {
        patterns.add(pattern);
    }

    /**
     * Removes a simple pattern from the list of filters
     *
     * @param pattern
     */
    public void removeFilter(String pattern) {
        patterns.remove(pattern);
    }

    public String getPatterns() {
        return Strings.collectionToDelimitedString(patterns, ",");
    }

    public void addFilterSettingParams(RestRequest request) {
        if (patterns.isEmpty() == false) {
            request.params().put(SETTINGS_FILTER_PARAM, getPatterns());
        }
    }

    public static Settings filterSettings(Params params, Settings settings) {
        String patterns = params.param(SETTINGS_FILTER_PARAM);
        Settings filteredSettings = settings;
        if (patterns != null && patterns.isEmpty() == false) {
            filteredSettings = SettingsFilter.filterSettings(patterns, filteredSettings);
        }
        return filteredSettings;
    }

    public static Settings filterSettings(String patterns, Settings settings) {
        String[] patternArray = Strings.delimitedListToStringArray(patterns, ",");
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(settings);
        List<String> simpleMatchPatternList = newArrayList();
        for (String pattern : patternArray) {
            if (Regex.isSimpleMatchPattern(pattern)) {
                simpleMatchPatternList.add(pattern);
            } else {
                builder.remove(pattern);
            }
        }
        if (!simpleMatchPatternList.isEmpty()) {
            String[] simpleMatchPatterns = simpleMatchPatternList.toArray(new String[simpleMatchPatternList.size()]);
            Iterator<Entry<String, String>> iterator = builder.internalMap().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> current = iterator.next();
                if (Regex.simpleMatch(simpleMatchPatterns, current.getKey())) {
                    iterator.remove();
                }
            }
        }
        return builder.build();
    }
}