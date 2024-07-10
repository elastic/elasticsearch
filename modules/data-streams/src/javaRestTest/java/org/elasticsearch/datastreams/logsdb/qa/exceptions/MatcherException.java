/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa.exceptions;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

/**
 * Generic base class for all types of mismatch errors.
 */
public class MatcherException extends Exception {

    public MatcherException(
        final XContentBuilder actualMappings,
        final Settings.Builder actualSettings,
        final XContentBuilder expectedMappings,
        final Settings.Builder expectedSettings,
        final String errorMessage
    ) {
        super(errorMessage(actualMappings, actualSettings, expectedMappings, expectedSettings, errorMessage));
    }

    private static String errorMessage(
        final XContentBuilder actualMappings,
        final Settings.Builder actualSettings,
        final XContentBuilder expectedMappings,
        final Settings.Builder expectedSettings,
        final String errorMessage
    ) {
        return "Error ["
            + errorMessage
            + "] "
            + "actual mappings ["
            + Strings.toString(actualMappings)
            + "] "
            + "actual settings ["
            + Strings.toString(actualSettings.build())
            + "] "
            + "expected mappings ["
            + Strings.toString(expectedMappings)
            + "] "
            + "expected settings ["
            + Strings.toString(expectedSettings.build())
            + "] ";
    }
}
