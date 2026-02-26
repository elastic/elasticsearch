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
import org.elasticsearch.core.Nullable;

import java.util.Arrays;
import java.util.List;

public enum StreamType {

    LOGS("logs"),
    LOGS_ECS("logs.ecs"),
    LOGS_OTEL("logs.otel");

    private final String streamName;

    StreamType(String streamName) {
        this.streamName = streamName;
    }

    public String getStreamName() {
        return streamName;
    }

    public boolean streamTypeIsEnabled(ProjectMetadata projectMetadata) {
        StreamsMetadata metadata = projectMetadata.custom(StreamsMetadata.TYPE, StreamsMetadata.EMPTY);
        return switch (this) {
            case LOGS -> metadata.isLogsEnabled();
            case LOGS_ECS -> metadata.isLogsECSEnabled();
            case LOGS_OTEL -> metadata.isLogsOTelEnabled();
        };
    }

    private boolean matchesStreamPrefix(String indexName) {
        if (indexName == null) {
            return false;
        }
        return indexName.startsWith(streamName + ".");
    }

    public static StreamType fromString(String text) {
        return switch (text) {
            case "logs.otel" -> LOGS_OTEL;
            case "logs.ecs" -> LOGS_ECS;
            case "logs" -> LOGS;
            default -> throw new IllegalArgumentException("Unknown stream type [" + text + "]");
        };
    }

    /**
     * For a given index name, return the StreamType that it matches exactly.
     */
    @Nullable
    public static StreamType exactEnabledStreamMatch(ProjectMetadata projectMetadata, String indexName) {
        return Arrays.stream(values())
            .filter(t -> t.streamTypeIsEnabled(projectMetadata))
            .filter(t -> t.getStreamName().equals(indexName))
            .findFirst()
            .orElse(null);
    }

    /**
     * For a given index name, return the enabled stream of which it would be part.
     *
     * For example, for an index 'logs.ecs.foo' if the LOGS_ECS stream type is enabled,
     * this will return LOGS_ECS. If LOGS_ECS were not enabled but LOGS was, then LOGS
     * would be returned. If neither were enabled, null would be returned.
     *
     * If no enabled stream type matches, returns null.
     */
    @Nullable
    public static StreamType enabledParentStreamOf(ProjectMetadata projectMetadata, String indexName) {
        // Check for exact names first, in which case indexing is allowed
        if (Arrays.stream(values())
            .filter(t -> t.streamTypeIsEnabled(projectMetadata))
            .anyMatch(t -> t.getStreamName().equals(indexName))) {
            return null;
        } else {
            // Check that any enabled stream type isn't targeted by the index
            List<StreamType> matchingStreamTypes = Arrays.stream(values())
                .filter(t -> t.streamTypeIsEnabled(projectMetadata))
                .filter(t -> t.matchesStreamPrefix(indexName))
                .toList();
            if (matchingStreamTypes.isEmpty()) {
                return null;
            } else if (matchingStreamTypes.size() == 1) {
                return matchingStreamTypes.getFirst();
            } else {
                // Try to return the most fine-grained stream type, otherwise `LOGS`
                return matchingStreamTypes.stream().filter(s -> s != LOGS).findFirst().orElse(LOGS);
            }
        }
    }
}
