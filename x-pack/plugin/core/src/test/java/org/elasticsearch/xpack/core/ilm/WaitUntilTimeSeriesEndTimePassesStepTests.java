/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.ToXContentObject;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class WaitUntilTimeSeriesEndTimePassesStepTests extends AbstractStepTestCase<WaitUntilTimeSeriesEndTimePassesStep> {

    @Override
    protected WaitUntilTimeSeriesEndTimePassesStep createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        Step.StepKey nextStepKey = randomStepKey();
        return new WaitUntilTimeSeriesEndTimePassesStep(stepKey, nextStepKey, Instant::now, client);
    }

    @Override
    protected WaitUntilTimeSeriesEndTimePassesStep mutateInstance(WaitUntilTimeSeriesEndTimePassesStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0 -> key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
        }
        return new WaitUntilTimeSeriesEndTimePassesStep(key, nextKey, Instant::now, client);
    }

    @Override
    protected WaitUntilTimeSeriesEndTimePassesStep copyInstance(WaitUntilTimeSeriesEndTimePassesStep instance) {
        return new WaitUntilTimeSeriesEndTimePassesStep(instance.getKey(), instance.getNextStepKey(), Instant::now, client);
    }

    public void testEvaluateCondition() {
        Instant currentTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        // These ranges are on the edge of each other temporal boundaries.
        Instant start1 = currentTime.minus(6, ChronoUnit.HOURS);
        Instant end1 = currentTime.minus(2, ChronoUnit.HOURS);
        Instant start2 = currentTime.minus(2, ChronoUnit.HOURS);
        Instant end2 = currentTime.plus(2, ChronoUnit.HOURS);

        String dataStreamName = "logs_my-app_prod";
        var clusterState = DataStreamTestHelper.getClusterStateWithDataStream(
            dataStreamName,
            List.of(Tuple.tuple(start1, end1), Tuple.tuple(start2, end2))
        );
        DataStream dataStream = clusterState.getMetadata().dataStreams().get(dataStreamName);

        WaitUntilTimeSeriesEndTimePassesStep step = new WaitUntilTimeSeriesEndTimePassesStep(
            randomStepKey(),
            randomStepKey(),
            () -> currentTime,
            client
        );
        {
            // end_time has lapsed already so condition must be met
            Index previousGeneration = dataStream.getIndices().get(0);

            step.evaluateCondition(clusterState.metadata(), previousGeneration, new AsyncWaitStep.Listener() {

                @Override
                public void onResponse(boolean complete, ToXContentObject infomationContext) {
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
            Index writeIndex = dataStream.getIndices().get(1);

            step.evaluateCondition(clusterState.metadata(), writeIndex, new AsyncWaitStep.Listener() {

                @Override
                public void onResponse(boolean complete, ToXContentObject infomationContext) {
                    assertThat(complete, is(false));
                    String information = Strings.toString(infomationContext);
                    assertThat(
                        information,
                        containsString(
                            "The [index.time_series.end_time] setting for index ["
                                + writeIndex.getName()
                                + "] is ["
                                + end2.toEpochMilli()
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

            Metadata newMetadata = Metadata.builder(clusterState.metadata()).put(indexMeta, true).build();
            step.evaluateCondition(newMetadata, indexMeta.getIndex(), new AsyncWaitStep.Listener() {

                @Override
                public void onResponse(boolean complete, ToXContentObject infomationContext) {
                    assertThat(complete, is(true));
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("Unexpected method call", e);
                }
            }, MASTER_TIMEOUT);
        }
    }
}
