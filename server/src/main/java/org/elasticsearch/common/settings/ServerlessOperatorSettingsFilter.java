/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class ServerlessOperatorSettingsFilter implements SettingsFilter {
    // fix AuthenticationField being in security plugin
    public static final String PRIVILEGE_CATEGORY_KEY = "_security_privilege_category";
    // fix AuthenticationField being in security plugin
    public static final String PRIVILEGE_CATEGORY_VALUE_OPERATOR = "operator";

    private final Set<String> patterns;
    private final ThreadContext threadContext;
    private final SettingsFilter esMainFilter;
    private final IndexScopedSettings indexScopedSettings;

    public ServerlessOperatorSettingsFilter(
        Collection<String> serverlessSettings,
        ThreadContext threadContext,
        SettingsFilter esMainFilter,
        IndexScopedSettings indexScopedSettings
    ) {
        this.patterns = Set.copyOf(serverlessSettings);
        this.threadContext = threadContext;
        this.esMainFilter = esMainFilter;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    public void addFilterSettingParams(RestRequest request) {

    }

    @Override
    public Settings filter(Settings settings) {
        Settings filter = esMainFilter.filter(settings);
        if (false == PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(threadContext.getHeader(PRIVILEGE_CATEGORY_KEY))) {
            // when a request is not operator, filter non-public settings
            return SettingsFilter.filterSettings(patterns, filter);
        }
        return filter;
    }

    @Override
    public void validateSettings(Settings settings) {
        if (false == PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(threadContext.getHeader(PRIVILEGE_CATEGORY_KEY))) {
            // when a request is not operator, do not allow to set operator only settings.
            List<String> list = settings.keySet()
                .stream()
                .filter(settingName -> indexScopedSettings.get(settingName).isServerlessPublic())
                .toList();
            if (false == list.isEmpty()) {
                throw new RuntimeException(
                    (list.size() == 1 ? "setting" : "settings") + " [" + Strings.collectionToDelimitedString(list, ",") + "]"
                );
            }

        }
    }
}
