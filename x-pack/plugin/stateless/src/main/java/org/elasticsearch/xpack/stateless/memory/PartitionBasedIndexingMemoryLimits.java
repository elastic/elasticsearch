/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.indices.IndexingMemoryLimits;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xpack.stateless.memory.partition.IndexBuffersPartition;
import org.elasticsearch.xpack.stateless.memory.partition.IndexingPressurePartition;

/**
 * Partition-based implementation of {@link IndexingMemoryLimits} for stateless deployments.
 * Back-pressure limits for {@link IndexingPressure} are set to
 * {@code IndexingPressurePartition.fraction × heapMax}, and the Lucene indexing buffer
 * size is set to {@code IndexBuffersPartition.fraction × heapMax}.
 *
 * <p>The single-operation limit ({@link #operationLimitBytes()}) retains the standard
 * settings-based value, as it is also used as the autoscaling signal denominator.
 */
public class PartitionBasedIndexingMemoryLimits implements IndexingMemoryLimits {

    private static final Logger logger = LogManager.getLogger(PartitionBasedIndexingMemoryLimits.class);

    private final long coordinatingLimitBytes;
    private final long primaryLimitBytes;
    private final long replicaLimitBytes;
    private final long operationLimitBytes;
    private final long indexBufferBytes;

    public PartitionBasedIndexingMemoryLimits(double indexingPressureFraction, double indexBuffersFraction, long operationLimitBytes) {
        long heapMaxBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        /*
            For consistency with the old setting defaults, the replica limit
            uses the entire partition, and the coordinating and primary
            limits are 2/3 of that.
         */
        long indexingPressurePartitionSizeBytes = (long) (heapMaxBytes * indexingPressureFraction);
        this.coordinatingLimitBytes = indexingPressurePartitionSizeBytes * 2 / 3;
        this.primaryLimitBytes = indexingPressurePartitionSizeBytes * 2 / 3;
        this.replicaLimitBytes = indexingPressurePartitionSizeBytes;
        this.operationLimitBytes = operationLimitBytes;
        /*
            For consistency with the old setting defaults, the buffer size is
            2/3 of the partition size, so there is room for the limit to
            be exceeded (we only throttle at 1.5x the buffer size, see
            org/elasticsearch/indices/IndexingMemoryController.java:402)
         */
        this.indexBufferBytes = (long) (heapMaxBytes * indexBuffersFraction) * 2 / 3;
        logger.info(
            "Indexing limits: coordinating={}, primary={}, replica={}, operation={}, buffers={}",
            ByteSizeValue.ofBytes(coordinatingLimitBytes),
            ByteSizeValue.ofBytes(primaryLimitBytes),
            ByteSizeValue.ofBytes(replicaLimitBytes),
            ByteSizeValue.ofBytes(operationLimitBytes),
            ByteSizeValue.ofBytes(indexBufferBytes)
        );
    }

    @Override
    public long coordinatingLimitBytes() {
        return coordinatingLimitBytes;
    }

    @Override
    public long primaryLimitBytes() {
        return primaryLimitBytes;
    }

    @Override
    public long replicaLimitBytes() {
        return replicaLimitBytes;
    }

    @Override
    public long operationLimitBytes() {
        return operationLimitBytes;
    }

    @Override
    public long indexBufferBytes() {
        return indexBufferBytes;
    }

    /** Constructs limits from the partition fraction settings present in {@code settings}. */
    public static PartitionBasedIndexingMemoryLimits fromSettings(org.elasticsearch.common.settings.Settings settings) {
        return new PartitionBasedIndexingMemoryLimits(
            IndexingPressurePartition.FRACTION_SETTING.get(settings).getAsRatio(),
            IndexBuffersPartition.FRACTION_SETTING.get(settings).getAsRatio(),
            IndexingPressure.MAX_OPERATION_SIZE.get(settings).getBytes()
        );
    }
}
