/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa.matchers;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class Messages {
    public static String formatErrorMessage(
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

    public static String prettyPrintArrays(final Object[] actualArray, final Object[] expectedArray) {
        return "actual: "
            + prettyPrintCollection(Arrays.asList(actualArray))
            + ", expected: "
            + prettyPrintCollection(Arrays.asList(expectedArray));
    }

    public static <T> String prettyPrintCollections(final Collection<T> actualList, final Collection<T> expectedList) {
        return "actual: " + prettyPrintCollection(actualList) + ", expected: " + prettyPrintCollection(expectedList);
    }

    private static <T> String prettyPrintCollection(final Collection<T> list) {
        return "[" + list.stream().map(Object::toString).collect(Collectors.joining(", ")) + "]";
    }
}
