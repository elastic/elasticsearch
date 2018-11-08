/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.function.LongSupplier;

/**
 * Checks whether the {@link RolloverStep} immediately prior to this step has successfully rolled over, including
 * attaching RolloverInfo necessary for subsequent steps. Without this step, sometimes the RolloverInfo is not attached
 * before the next step runs, causing an error.
 *
 * This step includes a timeout, and will error if the rollover has not completed after the timeout has passed.
 */
public class CheckRolloverStep extends AsyncWaitStep {
    public static final String NAME = "check_rollover";

    private static final Logger logger = LogManager.getLogger(CheckRolloverStep.class);
    static final long TIMEOUT_MILLIS = TimeValue.timeValueMinutes(10).millis();

    private LongSupplier nowSupplier;

    CheckRolloverStep(StepKey key, StepKey nextStepKey, Client client, LongSupplier nowSupplier) {
        super(key, nextStepKey, client);
        this.nowSupplier = nowSupplier;
    }

    @Override
    public void evaluateCondition(IndexMetaData indexMetaData, Listener listener) {
        String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(indexMetaData.getSettings());
        if (Strings.isNullOrEmpty(rolloverAlias)) {
            listener.onFailure(new IllegalStateException("setting [" + RolloverAction.LIFECYCLE_ROLLOVER_ALIAS
                + "] is not set on index [" + indexMetaData.getIndex().getName() + "]"));
            return;
        }
        RolloverInfo rolloverInfo = indexMetaData.getRolloverInfos().get(rolloverAlias);
        if (rolloverInfo == null) {
            logger.trace("{} index does not have rollover info yet", indexMetaData.getIndex());
            // Check if we've timed out.
            LifecycleExecutionState executionState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
            Long stepTime = executionState.getStepTime();

            assert NAME.equals(executionState.getStep()) : "the execution state must be on this step";
            if (stepTime == null) {
                listener.onFailure(new IllegalStateException("index [" + indexMetaData.getIndex().getName()  + "] has a null step_time"));
                return;
            }
            long millisSinceEnteringStep = nowSupplier.getAsLong() - stepTime;

            if (millisSinceEnteringStep > TIMEOUT_MILLIS) {
                listener.onFailure(new IllegalStateException("index [" + indexMetaData.getIndex().getName() + "] was not rolled over "+
                    "using the configured rollover alias [" + rolloverAlias + "] or a subsequent index was created outside of Index " +
                    "Lifecycle Management"));
                return;
            }

            listener.onResponse(false, null);
        } else {
            listener.onResponse(true, null);
        }

    }
}
