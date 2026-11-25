/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.view.ViewQuerySettings.WildcardsStop;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class ViewQuerySettingsTests extends ESTestCase {

    private static final String ADVICE = "; use that setting inside the view definition if you wish to change the view behaviour";
    private static final WildcardsStop CALLER_STOP = new WildcardsStop(null, "v");

    public void testWildcardsStoppedAtViewWarningForOneName() {
        assertThat(
            ViewQuerySettings.wildcardsStoppedAtViewWarning("wildcards_match_views", CALLER_STOP, "view", List.of("a"), List.of()),
            equalTo(
                "SET wildcards_match_views=true does not affect the contents of view [v], so its wildcards did not match view [a]" + ADVICE
            )
        );
    }

    public void testWildcardsStoppedAtViewWarningForSeveralNames() {
        assertThat(
            ViewQuerySettings.wildcardsStoppedAtViewWarning(
                "wildcards_match_datasets",
                CALLER_STOP,
                "dataset",
                List.of("a", "b"),
                List.of()
            ),
            equalTo(
                "SET wildcards_match_datasets=true does not affect the contents of view [v], so its wildcards did not match "
                    + "datasets [a], [b]"
                    + ADVICE
            )
        );
    }

    public void testWildcardsStoppedAtViewWarningCountsNamesBeyondTheLimit() {
        int limit = ViewQuerySettings.MAX_LISTED_NAMES_IN_VIEW_WARNINGS;
        int extra = between(1, 5);
        List<String> names = IntStream.range(0, limit + extra).mapToObj(i -> "n" + i).toList();
        String warning = ViewQuerySettings.wildcardsStoppedAtViewWarning("wildcards_match_views", CALLER_STOP, "view", names, List.of());
        String listed = String.join(", ", names.subList(0, limit).stream().map(n -> "[" + n + "]").toList());
        assertThat(
            warning,
            equalTo(
                "SET wildcards_match_views=true does not affect the contents of view [v], so its wildcards did not match views "
                    + listed
                    + " and ["
                    + extra
                    + "] more"
                    + ADVICE
            )
        );
    }

    public void testWildcardsStoppedAtViewWarningNamesTheSettingViewAndCircularViews() {
        assertThat(
            ViewQuerySettings.wildcardsStoppedAtViewWarning(
                "wildcards_match_views",
                new WildcardsStop("outer", "v"),
                "view",
                List.of("a"),
                List.of("outer", "v")
            ),
            equalTo(
                "SET wildcards_match_views=true in view [outer] does not affect the contents of view [v], so its wildcards did not "
                    + "match view [a]"
                    + ADVICE
                    + ", excluding views [outer], [v] from its wildcards since matching them would be a circular reference"
            )
        );
    }
}
