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

import java.util.ArrayList;
import java.util.List;

public enum StreamType {

    LOGS("logs"),
    LOGS_ECS("logs.ecs"),
    LOGS_OTEL("logs.otel");

    // an allocation-free version of values() for hot code
    private static final List<StreamType> VALUES = List.of(values());

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
    // n.b. this method is hot enough to warrant being unrolled as loops rather than written as streams
    public static StreamType exactEnabledStreamMatch(ProjectMetadata projectMetadata, String indexName) {
        for (StreamType type : VALUES) {
            if (type.streamTypeIsEnabled(projectMetadata) && type.getStreamName().equals(indexName)) {
                return type;
            }
        }
        return null;
    }

    /**
     * For a given index name, return the enabled stream of which it would be part.
     * <p>
     * For example, for an index 'logs.ecs.foo' if the LOGS_ECS stream type is enabled,
     * this will return LOGS_ECS. If LOGS_ECS were not enabled but LOGS was, then LOGS
     * would be returned. If neither were enabled, null would be returned.
     * <p>
     * If no enabled stream type matches, returns null.
     */
    @Nullable
    // n.b. this method is hot enough to warrant being unrolled as loops rather than written as streams
    public static StreamType enabledParentStreamOf(ProjectMetadata projectMetadata, String indexName) {
        // Check for exact names first, in which case indexing is allowed
        for (StreamType type : VALUES) {
            if (type.streamTypeIsEnabled(projectMetadata) && type.getStreamName().equals(indexName)) {
                return null;
            }
        }

        List<StreamType> matchingTypes = null;
        for (StreamType type : VALUES) {
            if (type.streamTypeIsEnabled(projectMetadata) && type.matchesStreamPrefix(indexName)) {
                if (matchingTypes == null) {
                    matchingTypes = new ArrayList<>(VALUES.size());
                }
                matchingTypes.add(type);
            }
        }
        if (matchingTypes == null) {
            return null;
        } else if (matchingTypes.size() == 1) {
            return matchingTypes.getFirst();
        } else {
            // Try to return the most fine-grained stream type, otherwise `LOGS`
            for (StreamType type : matchingTypes) {
                if (type != LOGS) {
                    return type;
                }
            }
            return LOGS;
        }
    }
}
