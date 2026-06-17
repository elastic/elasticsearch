/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Typed metadata emitted for a BUCKET grouping column, carrying the resolved interval and — for the
 * 4-arg date form — the start/end epoch-millis bounds of the requested range.
 * <p>
 * Two implementations exist: {@link DateInterval} for {@code date} / {@code date_nanos} fields and
 * {@link NumericInterval} for numeric fields. Both implement {@link ToXContentObject} so they can be
 * rendered directly wherever XContent output is produced.
 * <p>
 * The outer {@code "bucket"} namespace key in the rendered output mirrors the shape stored in the
 * ES|QL column {@code _meta} field, which allows multiple metadata providers (e.g. the
 * {@code "approximation"} metadata) to coexist in the same map without key collisions.
 */
public sealed interface BucketIntervalMetadata extends ToXContentObject permits BucketIntervalMetadata.DateInterval,
    BucketIntervalMetadata.NumericInterval {

    /**
     * Converts this metadata to the {@code Map<String, Object>} format expected by
     * {@link org.elasticsearch.xpack.esql.action.ColumnInfoImpl}. The returned map has the shape
     * {@code {"bucket": \{...\}}} and is safe for wire serialisation via
     * {@code StreamOutput.writeGenericMap}.
     */
    Map<String, Object> toColumnInfoMetaMap();

    /**
     * Metadata for a {@code date} or {@code date_nanos} BUCKET column.
     *
     * @param intervalSize  number of units in the resolved bucket interval (e.g. {@code 1} for monthly
     *                      buckets)
     * @param intervalUnit  canonical unit name produced by {@link org.elasticsearch.common.Rounding.Interval}
     *                      (e.g. {@code "month"}, {@code "day"}, {@code "hour"})
     * @param start         start of the requested range as epoch millis, present only when the 4-arg
     *                      BUCKET form is used; {@code null} for 2-arg BUCKET
     * @param end           end of the requested range as epoch millis, present only when the 4-arg
     *                      BUCKET form is used; {@code null} for 2-arg BUCKET
     */
    record DateInterval(long intervalSize, String intervalUnit, @Nullable Long start, @Nullable Long end)
        implements
            BucketIntervalMetadata {

        public DateInterval {
            if ((start == null) != (end == null)) {
                throw new IllegalArgumentException("start and end must both be null or both be non-null");
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.startObject("bucket");
            builder.field("interval", intervalSize);
            builder.field("unit", intervalUnit);
            if (start != null) {
                builder.field("start", start);
            }
            if (end != null) {
                builder.field("end", end);
            }
            builder.endObject();
            return builder.endObject();
        }

        @Override
        public Map<String, Object> toColumnInfoMetaMap() {
            if (start != null) {
                return Map.of("bucket", Map.of("interval", intervalSize, "unit", intervalUnit, "start", start, "end", end));
            }
            return Map.of("bucket", Map.of("interval", intervalSize, "unit", intervalUnit));
        }
    }

    /**
     * Metadata for a numeric BUCKET column.
     *
     * @param interval  the resolved bucket width; always a positive finite double
     */
    record NumericInterval(double interval) implements BucketIntervalMetadata {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.startObject("bucket");
            builder.field("interval", interval);
            builder.endObject();
            return builder.endObject();
        }

        @Override
        public Map<String, Object> toColumnInfoMetaMap() {
            return Map.of("bucket", Map.of("interval", interval));
        }
    }
}
