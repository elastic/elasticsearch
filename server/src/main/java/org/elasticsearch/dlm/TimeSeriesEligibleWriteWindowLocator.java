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
public class TimeSeriesEligibleWriteWindowLocator {

    /**
     * Returns the effective ILM policy name for the provided data stream based on the index template
     * that is associated with this data stream. If the ILM plugin is not available, this method returns null.
     * @param dataStream the requested data stream
     * @param projectMetadata the project metadata based on which we will resolve the data stream configuration
     * @return the effective ILM policy name or null
     */
    protected String getEffectiveIlmPolicy(DataStream dataStream, ProjectMetadata projectMetadata) {
        return null;
    };

    /**
     * Returns the duration of the eligible write window based on this ILM policy, or -1 if it is infinite, undefined or
     * the ILM plugin is not available.
     */
    protected long getEligibleWriteWindowFromPolicy(String policy, ProjectMetadata projectMetadata) {
        return -1;
    };

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
    public long getEligibleWriteWindowStart(
        DataStream dataStream,
        ProjectMetadata projectMetadata,
        DataStreamGlobalRetention globalRetention,
        long requestStartTimestamp
    ) {
        // Eligible write window is only applicable for time series data streams
        if (dataStream.getIndexMode() != IndexMode.TIME_SERIES || dataStream.isSystem()) {
            return -1;
        }
        String ilmPolicy = getEffectiveIlmPolicy(dataStream, projectMetadata);

        // If there is an effective ILM policy, we delegate the write window calculation to the ILM plugin.
        if (ilmPolicy != null) {
            return getEligibleWriteWindowFromPolicy(ilmPolicy, projectMetadata);
        }
        TimeValue writeWindow = null;
        if (dataStream.getDataLifecycle() != null && dataStream.getDataLifecycle().enabled()) {
            // Data stream lifecycle is executing the following actions in order, so the first read-only
            // configuration is the one that will determine the eligible write window.
            if (dataStream.getDataLifecycle().downsamplingRounds() != null) {
                // First, we check downsampling, downsampling configuration needs to have at least 1 round and the rounds
                // are sorted based on their after time. So, picking the first one is sufficient.
                writeWindow = dataStream.getDataLifecycle().downsamplingRounds().getFirst().after();
            } else if (dataStream.getDataLifecycle().frozenAfter() != null) {
                // Then we check the frozenAfter configuration.
                writeWindow = dataStream.getDataLifecycle().frozenAfter();
            } else {
                // Finally, we check the effective retention configuration
                writeWindow = dataStream.getDataLifecycle().getEffectiveDataRetention(globalRetention, dataStream.isInternal());
            }
        }
        return writeWindow == null ? -1 : requestStartTimestamp - writeWindow.getMillis();
    }
}
