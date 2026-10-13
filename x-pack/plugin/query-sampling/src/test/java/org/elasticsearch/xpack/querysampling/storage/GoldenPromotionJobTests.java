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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class GoldenPromotionJobTests extends ESTestCase {

    private final List<Integer> limits = new ArrayList<>();
    private final List<ActionListener<GoldenUpdater.Result>> held = new ArrayList<>();
    private boolean master = true;
    private boolean completeAtOnce = true;
    private GoldenUpdater.Result outcome = new GoldenUpdater.Result(1, 2, 0, 0, 3, 0, 0);
    private Exception failure;

    private GoldenPromotionJob job() {
        GoldenPromotionJob job = new GoldenPromotionJob((limit, listener) -> {
            limits.add(limit);
            if (failure != null) {
                listener.onFailure(failure);
            } else if (completeAtOnce) {
                listener.onResponse(outcome);
            } else {
                held.add(listener);
            }
        }, () -> master);
        job.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.GOLDEN_MAX_QUERIES.getKey(), 7).build(),
                Set.of(QuerySamplingSettings.GOLDEN_MAX_QUERIES)
            )
        );
        return job;
    }

    public void testOnlyTheElectedMasterGrowsTheDataset() {
        master = false;
        GoldenPromotionJob job = job();

        job.run();

        assertThat(limits.size(), equalTo(0));
        assertThat(job.promotions(), equalTo(0L));
    }

    public void testTheDatasetIsGrownWithTheLimitOfTheSetting() {
        GoldenPromotionJob job = job();

        job.run();

        assertThat(limits, equalTo(List.of(7)));
        assertThat(job.promotions(), equalTo(1L));
        assertThat(job.failed(), equalTo(0L));
    }

    public void testARoundThatMadeNoVersionIsNotCountedAsAPromotion() {
        outcome = new GoldenUpdater.Result(1, 0, 3, 0, 0, 0, 0);
        GoldenPromotionJob job = job();

        job.run();

        assertThat(limits.size(), equalTo(1));
        assertThat(job.promotions(), equalTo(0L));
    }

    public void testTheLimitFollowsTheSettingWhenItChanges() {
        GoldenPromotionJob job = new GoldenPromotionJob((limit, listener) -> {
            limits.add(limit);
            listener.onResponse(outcome);
        }, () -> true);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(QuerySamplingSettings.GOLDEN_MAX_QUERIES));
        job.watch(clusterSettings);
        job.run();

        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.GOLDEN_MAX_QUERIES.getKey(), 50).build());
        job.run();

        assertThat(limits, equalTo(List.of(1000, 50)));
    }

    public void testARoundThatFindsTheOneBeforeStillGoingDoesNothing() {
        completeAtOnce = false;
        GoldenPromotionJob job = job();

        job.run();
        job.run();

        assertThat(limits.size(), equalTo(1));
        held.get(0).onResponse(outcome);
        job.run();
        assertThat("once it is over the next one runs", limits.size(), equalTo(2));
    }

    public void testAFailedRoundIsCountedAndDoesNotStopTheNext() {
        failure = new IllegalStateException("no");
        GoldenPromotionJob job = job();

        job.run();
        failure = null;
        job.run();

        assertThat(job.failed(), equalTo(1L));
        assertThat(job.promotions(), equalTo(1L));
    }

    public void testAGrowerThatThrowsIsCountedAsFailed() {
        GoldenPromotionJob job = new GoldenPromotionJob((limit, listener) -> { throw new IllegalStateException("no"); }, () -> true);

        job.run();
        job.run();

        assertThat("the round is over, so the next one runs", job.failed(), equalTo(2L));
    }

    public void testTheIntervalIsNeverShorterThanASecond() {
        assertThat(GoldenPromotionJob.MIN_INTERVAL, equalTo(TimeValue.timeValueSeconds(1)));
    }
}
