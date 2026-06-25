/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.dlm;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;

/**
 * This class allows the caller to determine the relative start time of the eligible write window of a time-series data stream.
 * The eligible write window is determined by the lifecycle and the first read-only or delete/retention configuration that
 * is effective for the requested data stream.
 */
public interface TimeSeriesEligibleWriteWindowLocator {

    /**
     * Returns the effective ILM policy name for the provided data stream based on the index template
     * that is associated with this data stream. If the data stream has a data stream lifecycle configured,
     * it will take it into account and return null if the data stream lifecycle is effective. This choice
     * was made for performance reasons, since we need to resolve the settings for both the policy and the
     * `prefer_ilm` setting.
     * @param dataStream the requested data stream
     * @param projectMetadata the project metadata based on which we will resolve the data stream configuration
     * @return the effective ILM policy name or null
     */
    String getEffectiveIlmPolicy(DataStream dataStream, ProjectMetadata projectMetadata);

    /**
     * Returns the duration of the eligible write window based on this ILM policy, or -1 if it is infinite or undefined.
     */
    long getEligibleWriteWindowFromPolicy(String policy, ProjectMetadata projectMetadata);

    /**
     * Returns the start of the eligible write window based on the data stream configuration and its index template. The start time
     * is relative to the start time of the request. For example, if the request was received at 10:00:00 on the 24 of June, and the
     * eligible write window is 7 days, the returned timestamp will be 10:00:00 on the 17 of June.
     * @param dataStream the requested data stream
     * @param projectMetadata the project metadata based on which we will resolve the data stream configuration
     * @param globalRetention the global retention that can influence the effective data stream lifecycle retention
     * @param requestStartTimestamp the start of the request
     * @return the start of the eligible write, or -1 if the window is infinite
     */
    default long getEligibleWriteWindowStart(
        DataStream dataStream,
        ProjectMetadata projectMetadata,
        DataStreamGlobalRetention globalRetention,
        long requestStartTimestamp
    ) {
        if (dataStream.getIndexMode() != IndexMode.TIME_SERIES) {
            return -1;
        }
        String ilmPolicy = getEffectiveIlmPolicy(dataStream, projectMetadata);

        if (ilmPolicy != null) {
            return getEligibleWriteWindowFromPolicy(ilmPolicy, projectMetadata);
        }
        TimeValue writeWindow = null;
        if (dataStream.getDataLifecycle() != null && dataStream.getDataLifecycle().enabled()) {
            if (dataStream.getDataLifecycle().downsamplingRounds() != null) {
                writeWindow = dataStream.getDataLifecycle().downsamplingRounds().getFirst().after();
            } else if (dataStream.getDataLifecycle().frozenAfter() != null) {
                writeWindow = dataStream.getDataLifecycle().frozenAfter();
            } else {
                writeWindow = dataStream.getDataLifecycle().getEffectiveDataRetention(globalRetention, dataStream.isInternal());
            }
        }
        return writeWindow == null ? -1 : requestStartTimestamp - writeWindow.getMillis();
    }

    /**
     * A {@link TimeSeriesEligibleWriteWindowLocator} that only supports data stream lifecycle (DLM)
     */
    class DlmOnly implements TimeSeriesEligibleWriteWindowLocator {
        @Override
        public String getEffectiveIlmPolicy(DataStream dataStream, ProjectMetadata projectMetadata) {
            return null;
        }

        @Override
        public long getEligibleWriteWindowFromPolicy(String policy, ProjectMetadata projectMetadata) {
            return -1;
        }
    }
}
