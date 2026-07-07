/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xcontent.ToXContentObject;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class WaitUntilTimeSeriesEndTimePassesStepTests extends AbstractStepTestCase<WaitUntilTimeSeriesEndTimePassesStep> {

    @Override
    protected WaitUntilTimeSeriesEndTimePassesStep createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        Step.StepKey nextStepKey = randomStepKey();
        return new WaitUntilTimeSeriesEndTimePassesStep(stepKey, nextStepKey, Instant::now);
    }

    @Override
    protected WaitUntilTimeSeriesEndTimePassesStep mutateInstance(WaitUntilTimeSeriesEndTimePassesStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0 -> key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
        }
        return new WaitUntilTimeSeriesEndTimePassesStep(key, nextKey, Instant::now);
    }

    @Override
    protected WaitUntilTimeSeriesEndTimePassesStep copyInstance(WaitUntilTimeSeriesEndTimePassesStep instance) {
        return new WaitUntilTimeSeriesEndTimePassesStep(instance.getKey(), instance.getNextStepKey(), Instant::now);
    }

    /**
     * {@link org.elasticsearch.index.IndexMode#TSDB} is a preferred alternative to
     * {@link org.elasticsearch.index.IndexMode#TIME_SERIES} and must gate {@link WaitUntilTimeSeriesEndTimePassesStep}
     * identically, so the {@code index.mode} used for the time-series-specific assertions is randomized between the two.
     */
    public void testEvaluateCondition() {
        IndexMode mode = randomFrom(IndexMode.TIME_SERIES, IndexMode.TSDB);
        Instant currentTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        // These ranges are on the edge of each other temporal boundaries.
        Instant startTimeLapsed = currentTime.minus(6, ChronoUnit.HOURS);
        Instant endTimeLapsed = currentTime.minus(2, ChronoUnit.HOURS);
        Instant startTimeFuture = currentTime.minus(2, ChronoUnit.HOURS);
        Instant endTimeFuture = currentTime.plus(2, ChronoUnit.HOURS);

        WaitUntilTimeSeriesEndTimePassesStep step = new WaitUntilTimeSeriesEndTimePassesStep(
            randomStepKey(),
            randomStepKey(),
            () -> currentTime
        );

        {
            // end_time has lapsed already so condition must be met
            IndexMetadata indexMeta = createTimeSeriesIndexMetadata(mode, "ts-index-lapsed", startTimeLapsed, endTimeLapsed);
            ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMeta, true).build();
            ProjectState projectState = ClusterState.builder(ClusterName.DEFAULT)
                .putProjectMetadata(project)
                .build()
                .projectState(project.id());

            step.evaluateCondition(projectState, indexMeta, new AsyncWaitStep.Listener() {

                @Override
                public void onResponse(boolean complete, ToXContentObject informationContext) {
                    assertThat(complete, is(true));
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("Unexpected method call", e);
                }
            }, MASTER_TIMEOUT);
        }

        {
            // end_time is in the future
            IndexMetadata indexMeta = createTimeSeriesIndexMetadata(mode, "ts-index-future", startTimeFuture, endTimeFuture);
            ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMeta, true).build();
            ProjectState projectState = ClusterState.builder(ClusterName.DEFAULT)
                .putProjectMetadata(project)
                .build()
                .projectState(project.id());

            step.evaluateCondition(projectState, indexMeta, new AsyncWaitStep.Listener() {

                @Override
                public void onResponse(boolean complete, ToXContentObject informationContext) {
                    assertThat(complete, is(false));
                    String information = Strings.toString(informationContext);
                    assertThat(
                        information,
                        containsString(
                            "The [index.time_series.end_time] setting for index ["
                                + indexMeta.getIndex().getName()
                                + "] is ["
                                + endTimeFuture.toEpochMilli()
                                + "]. Waiting until the index's time series end time lapses before proceeding with action ["
                                + step.getKey().action()
                                + "] as the index can still accept writes."
                        )
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("Unexpected method call", e);
                }
            }, MASTER_TIMEOUT);
        }

        {
            // regular indices (non-ts) meet the step condition
            IndexMetadata indexMeta = IndexMetadata.builder(randomAlphaOfLengthBetween(10, 30))
                .settings(indexSettings(1, 1).put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current()).build())
                .build();

            ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMeta, true).build();
            ProjectState projectState = ClusterState.builder(ClusterName.DEFAULT)
                .putProjectMetadata(project)
                .build()
                .projectState(project.id());
            step.evaluateCondition(projectState, indexMeta, new AsyncWaitStep.Listener() {

                @Override
                public void onResponse(boolean complete, ToXContentObject informationContext) {
                    assertThat(complete, is(true));
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("Unexpected method call", e);
                }
            }, MASTER_TIMEOUT);
        }
    }

    private static IndexMetadata createTimeSeriesIndexMetadata(IndexMode mode, String indexName, Instant startTime, Instant endTime) {
        Settings settings = indexSettings(1, 1).put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
            .put(IndexSettings.MODE.getKey(), mode.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "uid")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(startTime))
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(endTime))
            .build();
        return IndexMetadata.builder(indexName).settings(settings).build();
    }
}
