/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.elasticsearch.xpack.esql.fetch.lifetime.FetchContextService.CONTEXT_KEEP_ALIVE;
import static org.hamcrest.Matchers.equalTo;

/**
 * The limits of {@link FetchContextService#CONTEXT_KEEP_ALIVE}.
 */
public class FetchContextServiceTests extends ESTestCase {
    /**
     * The default comes from {@link SearchService}, which owns its limits, so even a short one applies.
     */
    public void testKeepAliveDefaultsToTheSearchDefault() {
        assertThat(CONTEXT_KEEP_ALIVE.get(Settings.EMPTY), equalTo(SearchService.DEFAULT_KEEPALIVE_SETTING.get(Settings.EMPTY)));
        Settings settings = Settings.builder().put(SearchService.DEFAULT_KEEPALIVE_SETTING.getKey(), "500ms").build();
        assertThat(CONTEXT_KEEP_ALIVE.get(settings), equalTo(TimeValue.timeValueMillis(500)));
    }

    public void testKeepAliveIsAtLeastASecond() {
        Settings tooShort = Settings.builder().put(CONTEXT_KEEP_ALIVE.getKey(), "999ms").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CONTEXT_KEEP_ALIVE.get(tooShort));
        assertThat(e.getMessage(), equalTo("[esql.fetch.context_keep_alive] must be at least [1s], but was [999ms]"));

        Settings shortest = Settings.builder().put(CONTEXT_KEEP_ALIVE.getKey(), "1s").build();
        assertThat(CONTEXT_KEEP_ALIVE.get(shortest), equalTo(TimeValue.timeValueSeconds(1)));
    }

    public void testKeepAliveIsAtMostTheSearchLimit() {
        Settings settings = Settings.builder()
            .put(CONTEXT_KEEP_ALIVE.getKey(), "2h")
            .put(SearchService.MAX_KEEPALIVE_SETTING.getKey(), "1h")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CONTEXT_KEEP_ALIVE.get(settings));
        assertThat(
            e.getMessage(),
            equalTo("[esql.fetch.context_keep_alive] must be at most [1h], the value of [search.max_keep_alive], but was [2h]")
        );
    }

    /**
     * Lowering the search limit below a keep-alive set on its own fails, like lowering it below the default keep-alive of
     * searches.
     */
    public void testLoweringTheSearchLimitBelowTheKeepAliveFails() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(CONTEXT_KEEP_ALIVE.getKey(), "10m").build(),
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(CONTEXT_KEEP_ALIVE))
        );
        clusterSettings.initializeAndWatch(CONTEXT_KEEP_ALIVE, value -> {});

        Settings lowerLimit = Settings.builder().put(SearchService.MAX_KEEPALIVE_SETTING.getKey(), "5m").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.validateUpdate(lowerLimit));
        assertThat(
            e.getCause().getMessage(),
            equalTo("[esql.fetch.context_keep_alive] must be at most [5m], the value of [search.max_keep_alive], but was [10m]")
        );
    }
}
