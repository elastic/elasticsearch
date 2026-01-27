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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

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

    public boolean matchesStreamPrefix(String indexName) {
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

    public static Set<StreamType> getEnabledStreamTypesForProject(ProjectMetadata projectMetadata) {
        return Arrays.stream(values())
            .filter(t -> t.streamTypeIsEnabled(projectMetadata))
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(StreamType.class)));
    }

}
