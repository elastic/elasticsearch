/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class TimeSeriesDimensionsMetadataAccess {

    public static final String TIME_SERIES_DIMENSIONS_METADATA_KEY = "time_series_dimensions";

    public static void addToCustomMetadata(IndexMetadata.Builder indexMetadataBuilder, List<String> timeSeriesDimensions) {
        indexMetadataBuilder.putCustom(TIME_SERIES_DIMENSIONS_METADATA_KEY, toCustomMetadata(timeSeriesDimensions));
    }

    public static void addToCustomMetadata(
        ImmutableOpenMap.Builder<String, Map<String, String>> customMetadataBuilder,
        List<String> timeSeriesDimensions
    ) {
        customMetadataBuilder.put(TIME_SERIES_DIMENSIONS_METADATA_KEY, toCustomMetadata(timeSeriesDimensions));
    }

    static Map<String, String> toCustomMetadata(List<String> timeSeriesDimensions) {
        return Map.of("includes", String.join(",", timeSeriesDimensions));
    }

    public static void transferCustomMetadata(IndexMetadata sourceIndexMetadata, IndexMetadata.Builder targetIndexMetadataBuilder) {
        Map<String, String> metadata = sourceIndexMetadata.getCustomData(TIME_SERIES_DIMENSIONS_METADATA_KEY);
        if (metadata != null) {
            targetIndexMetadataBuilder.putCustom(TIME_SERIES_DIMENSIONS_METADATA_KEY, metadata);
        }
    }

    public static List<String> fromCustomMetadata(Map<String, ? extends Map<String, String>> customMetadata) {
        return fromTimeSeriesDimensionsMetadata(customMetadata.get(TIME_SERIES_DIMENSIONS_METADATA_KEY));
    }

    public static List<String> fromCustomMetadata(IndexMetadata indexMetadata) {
        return fromTimeSeriesDimensionsMetadata(indexMetadata.getCustomData(TIME_SERIES_DIMENSIONS_METADATA_KEY));
    }

    private static List<String> fromTimeSeriesDimensionsMetadata(Map<String, String> timeSeriesDimensionsMetadata) {
        if (timeSeriesDimensionsMetadata == null) {
            return List.of();
        }
        String includes = timeSeriesDimensionsMetadata.get("includes");
        if (includes == null || includes.isEmpty()) {
            return List.of();
        }
        return List.of(includes.split(","));
    }
}
