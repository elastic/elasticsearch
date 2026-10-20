/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class ViewQuerySettingsTests extends ESTestCase {

    private static final String ADVICE = "; use that setting inside the view definition if you wish to change the view behaviour";

    public void testWildcardsStoppedAtViewWarningForOneName() {
        assertThat(
            ViewQuerySettings.wildcardsStoppedAtViewWarning("wildcards_match_views", "v", "view", List.of("a")),
            equalTo(
                "SET wildcards_match_views=true does not affect the contents of view [v], so its wildcards did not match view [a]" + ADVICE
            )
        );
    }

    public void testWildcardsStoppedAtViewWarningForSeveralNames() {
        assertThat(
            ViewQuerySettings.wildcardsStoppedAtViewWarning("wildcards_match_datasets", "v", "dataset", List.of("a", "b")),
            equalTo(
                "SET wildcards_match_datasets=true does not affect the contents of view [v], so its wildcards did not match "
                    + "datasets [a], [b]"
                    + ADVICE
            )
        );
    }

    public void testWildcardsStoppedAtViewWarningCountsNamesBeyondTheLimit() {
        int limit = ViewQuerySettings.MAX_LISTED_IN_WILDCARDS_WARNING;
        int extra = between(1, 5);
        List<String> names = IntStream.range(0, limit + extra).mapToObj(i -> "n" + i).toList();
        String warning = ViewQuerySettings.wildcardsStoppedAtViewWarning("wildcards_match_views", "v", "view", names);
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
}
