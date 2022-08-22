/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.step.info.SingleMessageFieldInfo;

import java.util.Locale;

/**
 * Some actions cannot be executed on a data stream's write index (eg. `searchable-snapshot`). This step checks if the managed index is
 * part of a data stream, in which case it will check it's not the write index. If the managed index is the write index of a data stream
 * this step will wait until that's not the case (ie. rolling over the data stream will create a new index as the data stream's write
 * index and this step will be able to complete)
 */
public class CheckNotDataStreamWriteIndexStep extends ClusterStateWaitStep {

    public static final String NAME = "check-not-write-index";

    private static final Logger logger = LogManager.getLogger(CheckNotDataStreamWriteIndexStep.class);

    CheckNotDataStreamWriteIndexStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        Metadata metadata = clusterState.metadata();
        IndexMetadata indexMetadata = metadata.index(index);
        String indexName = index.getName();

        if (indexMetadata == null) {
            String errorMessage = String.format(
                Locale.ROOT,
                "[%s] lifecycle action for index [%s] executed but index no longer exists",
                getKey().getAction(),
                indexName
            );
            // Index must have been since deleted
            logger.debug(errorMessage);
            return new Result(false, new SingleMessageFieldInfo(errorMessage));
        }

        String policyName = indexMetadata.getLifecyclePolicyName();
        IndexAbstraction indexAbstraction = clusterState.metadata().getIndicesLookup().get(indexName);
        assert indexAbstraction != null : "invalid cluster metadata. index [" + indexName + "] was not found";
        IndexAbstraction.DataStream dataStream = indexAbstraction.getParentDataStream();
        if (dataStream != null) {
            assert dataStream.getWriteIndex() != null : dataStream.getName() + " has no write index";
            if (dataStream.getWriteIndex().equals(index)) {
                String errorMessage = String.format(
                    Locale.ROOT,
                    "index [%s] is the write index for data stream [%s], pausing "
                        + "ILM execution of lifecycle [%s] until this index is no longer the write index for the data stream via manual or "
                        + "automated rollover",
                    indexName,
                    dataStream.getName(),
                    policyName
                );
                logger.debug(errorMessage);
                return new Result(false, new SingleMessageFieldInfo(errorMessage));
            }
        }

        return new Result(true, null);
    }
}
