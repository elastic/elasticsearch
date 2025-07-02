/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.streams;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.StreamsMetadata;

import java.util.Set;

public class StreamsPermissionsUtils {

    private static volatile StreamsPermissionsUtils INSTANCE = null;

    // Visible for testing only
    StreamsPermissionsUtils() {}

    public static StreamsPermissionsUtils getInstance() {
        if (INSTANCE == null) {
            synchronized (StreamsPermissionsUtils.class) {
                if (INSTANCE == null) {
                    INSTANCE = new StreamsPermissionsUtils();
                }
            }
        }
        return INSTANCE;
    }

    public void throwIfRetrouteToSubstreamNotAllowed(ProjectMetadata projectMetadata, Set<String> indexHistory, String destination)
        throws IllegalArgumentException {
        for (StreamType streamType : StreamType.values()) {
            String streamName = streamType.getStreamName();
            if (streamTypeIsEnabled(streamType, projectMetadata)
                && destination.startsWith(streamName + ".")
                && indexHistory.contains(streamName) == false) {
                throw new IllegalArgumentException(
                    "Cannot reroute to substream ["
                        + destination
                        + "] as only the stream itself can reroute to substreams. "
                        + "Please reroute to the stream ["
                        + streamName
                        + "] instead."
                );
            }
        }
    }

    public boolean streamTypeIsEnabled(StreamType streamType, ProjectMetadata projectMetadata) {
        StreamsMetadata metadata = projectMetadata.custom(StreamsMetadata.TYPE, StreamsMetadata.EMPTY);
        return switch (streamType) {
            case LOGS -> metadata.isLogsEnabled();
        };
    }

}
