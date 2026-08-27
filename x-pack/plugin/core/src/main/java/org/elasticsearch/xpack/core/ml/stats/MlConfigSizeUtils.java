/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Helpers for measuring approximate serialized sizes of ML config fields.
 */
public final class MlConfigSizeUtils {

    public static final long SIZE_MEASUREMENT_FAILURE = -1L;

    private static final Logger logger = LogManager.getLogger(MlConfigSizeUtils.class);

    private MlConfigSizeUtils() {}

    public static long stringLength(@Nullable String value) {
        return value == null ? 0L : value.getBytes(StandardCharsets.UTF_8).length;
    }

    public static long collectionCount(@Nullable Collection<?> collection) {
        return collection == null ? 0L : collection.size();
    }

    public static long stringCollectionTotalLength(@Nullable Collection<String> values) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        long total = 0L;
        for (String value : values) {
            total += stringLength(value);
        }
        return total;
    }

    public static long stringArrayTotalLength(@Nullable String[] values) {
        if (values == null || values.length == 0) {
            return 0L;
        }
        long total = 0L;
        for (String value : values) {
            total += stringLength(value);
        }
        return total;
    }

    public static long mapApproxSizeBytes(@Nullable Map<String, ?> map) {
        if (map == null || map.isEmpty()) {
            return 0L;
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(map);
            return BytesReference.bytes(builder).length();
        } catch (IOException e) {
            logger.debug("Failed to measure approximate map size for config size telemetry", e);
            return SIZE_MEASUREMENT_FAILURE;
        }
    }

    public static long toXContentApproxSizeBytes(@Nullable ToXContentObject object) {
        if (object == null) {
            return 0L;
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            object.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder).length();
        } catch (IOException e) {
            logger.debug("Failed to measure approximate ToXContentObject size for config size telemetry", e);
            return SIZE_MEASUREMENT_FAILURE;
        }
    }

    public static long toXContentFragmentApproxSizeBytes(@Nullable ToXContentFragment fragment) {
        if (fragment == null) {
            return 0L;
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            fragment.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder).length();
        } catch (IOException e) {
            logger.debug("Failed to measure approximate ToXContentFragment size for config size telemetry", e);
            return SIZE_MEASUREMENT_FAILURE;
        }
    }

    public static long queryBuilderApproxSizeBytes(@Nullable QueryBuilder query) {
        if (query == null) {
            return 0L;
        }
        try {
            return mapApproxSizeBytes(XContentObjectTransformer.queryBuilderTransformer(NamedXContentRegistry.EMPTY).toMap(query));
        } catch (IOException e) {
            logger.debug("Failed to measure approximate QueryBuilder size for config size telemetry", e);
            return SIZE_MEASUREMENT_FAILURE;
        }
    }

    public static long sumSizeBytes(long currentTotal, long nextSize) {
        if (nextSize < 0L) {
            return SIZE_MEASUREMENT_FAILURE;
        }
        if (currentTotal < 0L) {
            return SIZE_MEASUREMENT_FAILURE;
        }
        return currentTotal + nextSize;
    }

    public static Map<String, Object> configSizesMap(Map<String, SizeHistogramAccumulator> accumulators) {
        Map<String, Object> configSizes = new LinkedHashMap<>();
        accumulators.forEach((key, accumulator) -> configSizes.put(key, accumulator.asMap()));
        return configSizes;
    }
}
