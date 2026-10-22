/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class GoldenPromotionJobTests extends ESTestCase {

    private final List<Integer> limits = new ArrayList<>();

    private final List<ActionListener<GoldenUpdater.Result>> running = new ArrayList<>();

    private GoldenPromotionJob job() {
        GoldenPromotionJob job = new GoldenPromotionJob((limit, listener) -> {
            limits.add(limit);
            running.add(listener);
        });
        job.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.GOLDEN_MAX_QUERIES.getKey(), 250).build(),
                Set.of(QuerySamplingSettings.GOLDEN_MAX_QUERIES)
            )
        );
        return job;
    }

    public void testARunWhileThePreviousOneIsStillGoingDoesNothing() {
        GoldenPromotionJob job = job();

        job.run();
        job.run();

        assertThat(running.size(), equalTo(1));
        assertThat("the limit is the maximum number of golden queries", limits, equalTo(List.of(250)));
    }

    public void testANewRunStartsOnceThePreviousOneEndedWhetherItSucceededOrFailed() {
        GoldenPromotionJob job = job();

        job.run();
        running.get(0).onResponse(new GoldenUpdater.Result(1, 2, 3, 0, 0, 0, 0));
        job.run();
        assertThat(running.size(), equalTo(2));

        running.get(1).onFailure(new IllegalStateException("failed"));
        job.run();
        assertThat(running.size(), equalTo(3));
    }
}
