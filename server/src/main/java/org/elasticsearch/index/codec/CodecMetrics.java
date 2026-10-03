/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;

import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Counts failures thrown by Lucene codec formats. Recorded from two hooks: {@link MetricingCodec} on the write side and
 * {@link org.elasticsearch.common.lucene.index.ElasticsearchLeafReader} on the read side, which sees the per-field accessors and random
 * access stored fields but not the iterators they return or the sequential stored fields reader.
 * <p>
 * The counter is meant to alarm on any codec failure, so it carries only what an alert needs: the format that failed, resolved to the
 * concrete class where possible ({@link Format#formatName}), and the {@link MetricAttributes#ERROR_TYPE}. Whether the failure was on the
 * read or the write path, and in which index, is left to the logged stack trace.
 */
public class CodecMetrics {

    public static final String CODEC_FAILURE_TOTAL = "es.codec.failure.total";

    public static final String FORMAT_ATTRIBUTE = "es_codec_format";

    public static final CodecMetrics NOOP = new CodecMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    /** The family of codec format a failure came from. */
    public enum Format {
        POSTINGS,
        DOC_VALUES,
        STORED_FIELDS,
        KNN_VECTORS,
        POINTS,
        NORMS;

        /** The value recorded when the concrete format cannot be resolved. */
        public String label() {
            return name().toLowerCase(Locale.ROOT);
        }

        public String formatName(@Nullable Codec codec, @Nullable FieldInfo field) {
            String name = switch (this) {
                case POSTINGS -> perField(field, PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY, PostingsFormat::forName);
                case DOC_VALUES -> perField(field, PerFieldDocValuesFormat.PER_FIELD_FORMAT_KEY, DocValuesFormat::forName);
                case KNN_VECTORS -> perField(field, PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY, KnnVectorsFormat::forName);
                case STORED_FIELDS -> codec == null ? null : codec.storedFieldsFormat().getClass().getSimpleName();
                case POINTS -> codec == null ? null : codec.pointsFormat().getClass().getSimpleName();
                case NORMS -> codec == null ? null : codec.normsFormat().getClass().getSimpleName();
            };
            // An anonymous format class has an empty simple name, which is no better than no name.
            return name == null || name.isEmpty() ? label() : name;
        }

        /**
         * The per-field dispatcher stamps the chosen format's SPI name on the field, and a segment cannot be written or opened unless that
         * name resolves, so the lookup cannot fail here.
         */
        @Nullable
        private static String perField(@Nullable FieldInfo field, String attribute, Function<String, ?> forName) {
            String spiName = field == null ? null : field.getAttribute(attribute);
            return spiName == null ? null : forName.apply(spiName).getClass().getSimpleName();
        }
    }

    private final LongCounter failures;

    public CodecMetrics(MeterRegistry meterRegistry) {
        failures = meterRegistry.registerLongCounter(CODEC_FAILURE_TOTAL, "Number of failures thrown by Lucene codec formats", "unit");
    }

    /**
     * Counts one failure of {@code format}, unless it is an {@link AlreadyClosedException}: a reader or writer used after its shard closed
     * is a lifecycle race, not a codec failure, and would otherwise dominate the counter.
     *
     * @param codec the codec of the segment, used to resolve the concrete format; see {@link Format#formatName}
     * @param field the field the failing call was about, used to resolve the concrete format; see {@link Format#formatName}
     */
    public void onFailure(Format format, @Nullable Codec codec, @Nullable FieldInfo field, Throwable t) {
        if (ExceptionsHelper.unwrapCause(t) instanceof AlreadyClosedException) {
            return;
        }
        failures.incrementBy(
            1,
            Map.of(FORMAT_ATTRIBUTE, format.formatName(codec, field), MetricAttributes.ERROR_TYPE, MetricAttributes.errorType(t))
        );
    }
}
