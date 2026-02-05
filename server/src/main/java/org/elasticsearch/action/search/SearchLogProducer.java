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
import org.elasticsearch.common.logging.action.ActionLoggerProducer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.logging.Level;

import java.util.Arrays;
import java.util.function.Predicate;

import static org.elasticsearch.common.logging.action.ActionLogger.ACTION_LOGGER_SETTINGS_PREFIX;

public class SearchLogProducer implements ActionLoggerProducer<SearchLogContext> {

    public static final String LOGGER_NAME = "search.actionlog";
    public static final String[] NEVER_MATCH = new String[] { "*", "-*" };

    private boolean logSystemSearches = false;
    private final Predicate<String> systemChecker;

    public static final Setting<Boolean> SEARCH_LOGGER_LOG_SYSTEM = Setting.boolSetting(
        ACTION_LOGGER_SETTINGS_PREFIX + "search.include_system_indices",
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
    public ESLogMessage produce(SearchLogContext context, ActionLoggingFields additionalFields) {
        ESLogMessage msg = produceCommon(context, additionalFields);
        msg.field("query", context.getQuery());
        msg.field("indices", context.getIndices());
        msg.field("hits", context.getHits());
        if (context.isSystemSearch(systemChecker)) {
            msg.field("is_system", true);
        }
        return msg;
    }

    @Override
    public Level logLevel(SearchLogContext context, Level defaultLevel) {
        if (Arrays.equals(NEVER_MATCH, context.getIndexNames())) {
            // Exclude no-match pattern searches, there's not much use in them
            return Level.OFF;
        }
        // Exclude system searches, based on option
        if (logSystemSearches == false && context.isSystemSearch(systemChecker)) {
            return Level.OFF;
        }
        return defaultLevel;
    }

    @Override
    public String loggerName() {
        return LOGGER_NAME;
    }

    public void setLogSystemSearches(boolean logSystemSearches) {
        this.logSystemSearches = logSystemSearches;
    }
}
