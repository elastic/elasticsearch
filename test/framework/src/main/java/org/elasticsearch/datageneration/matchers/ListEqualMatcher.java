/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.matchers;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.List;

import static org.elasticsearch.datageneration.matchers.Messages.formatErrorMessage;
import static org.elasticsearch.datageneration.matchers.Messages.prettyPrintCollections;

public class ListEqualMatcher extends GenericEqualsMatcher<List<?>> {
    public ListEqualMatcher(
        final XContentBuilder actualMappings,
        final Settings.Builder actualSettings,
        final XContentBuilder expectedMappings,
        final Settings.Builder expectedSettings,
        final List<?> actual,
        final List<?> expected,
        final boolean ignoringSort
    ) {
        super(actualMappings, actualSettings, expectedMappings, expectedSettings, actual, expected, ignoringSort);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MatchResult match() {
        return matchListEquals((List<Object>) actual, (List<Object>) expected, ignoringSort);
    }

    private MatchResult matchListEquals(final List<Object> actualList, final List<Object> expectedList, boolean ignoreSorting) {
        if (actualList.size() != expectedList.size()) {
            return MatchResult.noMatch(
                formatErrorMessage(
                    actualMappings,
                    actualSettings,
                    expectedMappings,
                    expectedSettings,
                    "List lengths do not match, " + prettyPrintCollections(actualList, expectedList)
                )
            );
        }
        if (ignoreSorting) {
            return matchListsEqualIgnoringSorting(actualList, expectedList)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Lists do not match when ignoring sort order, " + prettyPrintCollections(actualList, expectedList)
                    )
                );
        } else {
            return matchListsEqualExact(actualList, expectedList)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Lists do not match exactly, " + prettyPrintCollections(actualList, expectedList)
                    )
                );
        }
    }

    private static boolean matchListsEqualIgnoringSorting(final List<Object> actualList, final List<Object> expectedList) {
        return actualList.containsAll(expectedList) && expectedList.containsAll(actualList);
    }

    private static <T> boolean matchListsEqualExact(List<Object> actualList, List<Object> expectedList) {
        for (int i = 0; i < actualList.size(); i++) {
            boolean isEqual = actualList.get(i).equals(expectedList.get(i));
            if (isEqual == false) {
                return false;
            }
        }
        return true;
    }
}
