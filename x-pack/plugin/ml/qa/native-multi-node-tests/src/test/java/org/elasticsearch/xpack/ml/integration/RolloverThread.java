/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequestBuilder;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

class RolloverThread extends Thread {

    private static final Logger logger = LogManager.getLogger(RolloverThread.class);

    private final CheckedConsumer<Long, InterruptedException> sleepFunction;
    private final AtomicBoolean active = new AtomicBoolean(true);
    private int rolloversPerformedCount = 0;

    RolloverThread() {
        this(Thread::sleep);
    }

    RolloverThread(CheckedConsumer<Long, InterruptedException> sleepFunction) {
        this.sleepFunction = Objects.requireNonNull(sleepFunction);
    }

    @Override
    public void run() {
        while (active.get()) {
            logger.warn("About to start rollover no. " + (rolloversPerformedCount++));
            rolloverMlStateIndex();
            try {
                sleepFunction.accept(1000L);
            } catch (InterruptedException e) {
                logger.warn("RolloverRunnable.run() interrupted", e);
                active.set(false);
            }
        }
    }

    void deactivate() {
        active.set(false);
    }

    private static void rolloverMlStateIndex() {
        String writeAlias = AnomalyDetectorsIndex.jobStateIndexWriteAlias();
        String newIndexName;
        try {
            newIndexName = getNewIndexName();
        } catch (IndexNotFoundException e) {
            logger.info(e.getMessage(), e);
            return;
        }
        RolloverRequestBuilder rolloverRequestBuilder = ESIntegTestCase.client().admin().indices().prepareRolloverIndex(writeAlias);
        if (newIndexName != null) {
            rolloverRequestBuilder.setNewIndexName(newIndexName);
        }
        rolloverRequestBuilder.waitForActiveShards(ActiveShardCount.ALL);
        RolloverResponse rolloverResponse = rolloverRequestBuilder.get();
        assertThat(rolloverResponse.isRolledOver(), is(true));
        assertThat(rolloverResponse.isShardsAcknowledged(), is(true));
    }

    @Nullable
    private static String getNewIndexName() {
        String initialMlStateIndex = AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX;
        String writeAlias = AnomalyDetectorsIndex.jobStateIndexWriteAlias();
        GetAliasesResponse getAliasesResponse =
            ESIntegTestCase.client().admin().indices().prepareGetAliases(writeAlias).setIndices(initialMlStateIndex).get();
        List<AliasMetaData> aliases = getAliasesResponse.getAliases().get(initialMlStateIndex);
        if (aliases == null) {
            return null;
        }
        if (aliases.stream().filter(a -> writeAlias.equals(a.alias())).findFirst().isEmpty()) {
            return null;
        }
        return initialMlStateIndex + "-000002";
    }
}
