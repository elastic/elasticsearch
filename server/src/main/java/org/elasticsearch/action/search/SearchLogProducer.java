/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.activity.ActivityLogProducer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.ActionLoggingFields;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Predicate;

import static org.elasticsearch.common.logging.activity.ActivityLogger.ACTIVITY_LOGGER_SETTINGS_PREFIX;

public class SearchLogProducer implements ActivityLogProducer<SearchLogContext> {

    public static final String LOGGER_NAME = "search.activitylog";
    public static final String[] NEVER_MATCH = new String[] { "*", "-*" };

    private boolean logSystemSearches = false;
    private final Predicate<String> systemChecker;

    public static final Setting<Boolean> SEARCH_LOGGER_LOG_SYSTEM = Setting.boolSetting(
        ACTIVITY_LOGGER_SETTINGS_PREFIX + "search.include_system_indices",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    @SuppressWarnings("this-escape")
    public SearchLogProducer(ClusterSettings settings, Predicate<String> systemChecker) {
        this.systemChecker = systemChecker;
        settings.initializeAndWatch(SEARCH_LOGGER_LOG_SYSTEM, this::setLogSystemSearches);
    }

    @Override
    public Optional<ESLogMessage> produce(SearchLogContext context, ActionLoggingFields additionalFields) {
        if (Arrays.equals(NEVER_MATCH, context.getIndexNames())) {
            // Exclude no-match pattern searches, there's not much use in them
            return Optional.empty();
        }
        // Exclude system searches, based on option
        if (logSystemSearches == false && context.isSystemSearch(systemChecker)) {
            return Optional.empty();
        }
        ESLogMessage msg = produceCommon(context, additionalFields);
        msg.field(ES_FIELDS_PREFIX + "query", context.getQuery());
        msg.field(ES_FIELDS_PREFIX + "indices", context.getIndices());
        msg.field(ES_FIELDS_PREFIX + "hits", context.getHits());
        if (context.isSystemSearch(systemChecker)) {
            msg.field(ES_FIELDS_PREFIX + "is_system", true);
        }
        return Optional.of(msg);
    }

    @Override
    public String loggerName() {
        return LOGGER_NAME;
    }

    public void setLogSystemSearches(boolean logSystemSearches) {
        this.logSystemSearches = logSystemSearches;
    }
}
