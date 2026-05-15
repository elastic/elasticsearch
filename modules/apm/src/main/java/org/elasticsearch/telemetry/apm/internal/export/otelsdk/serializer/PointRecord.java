/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk.serializer;

import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Wire representation of an OTel metric point. Three variants — long, double, histogram — each
 * carrying only the fields it needs. The variant is tagged on the wire by a {@code "kind"} field
 * written first, which the {@link #parse(XContentParser)} dispatcher reads to pick the constructor.
 */
sealed interface PointRecord extends ToXContentObject {

    String KIND = "kind";
    String START = "start_epoch_nanos";
    String EPOCH = "epoch_nanos";
    String ATTRIBUTES = "attributes";
    String VALUE = "value";
    String SUM = "sum";
    String COUNT = "count";
    String HAS_MIN = "has_min";
    String MIN = "min";
    String HAS_MAX = "has_max";
    String MAX = "max";
    String BOUNDARIES = "boundaries";
    String COUNTS = "counts";

    String KIND_LONG = "LONG";
    String KIND_DOUBLE = "DOUBLE";
    String KIND_HISTOGRAM = "HISTOGRAM";

    long startEpochNanos();

    long epochNanos();

    List<AttributeRecord> attributes();

    record LongPoint(long startEpochNanos, long epochNanos, List<AttributeRecord> attributes, long value) implements PointRecord {

        static LongPoint from(LongPointData p) {
            return new LongPoint(
                p.getStartEpochNanos(),
                p.getEpochNanos(),
                AttributeRecord.fromAttributes(p.getAttributes()),
                p.getValue()
            );
        }

        LongPointData to() {
            return LongPointData.create(startEpochNanos, epochNanos, AttributeRecord.toAttributes(attributes), value);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
            b.startObject();
            b.field(KIND, KIND_LONG);
            b.field(START, startEpochNanos);
            b.field(EPOCH, epochNanos);
            AttributeRecord.writeArray(b, ATTRIBUTES, attributes, params);
            b.field(VALUE, value);
            b.endObject();
            return b;
        }
    }

    record DoublePoint(long startEpochNanos, long epochNanos, List<AttributeRecord> attributes, double value) implements PointRecord {

        static DoublePoint from(DoublePointData p) {
            return new DoublePoint(
                p.getStartEpochNanos(),
                p.getEpochNanos(),
                AttributeRecord.fromAttributes(p.getAttributes()),
                p.getValue()
            );
        }

        DoublePointData to() {
            return DoublePointData.create(startEpochNanos, epochNanos, AttributeRecord.toAttributes(attributes), value, List.of());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
            b.startObject();
            b.field(KIND, KIND_DOUBLE);
            b.field(START, startEpochNanos);
            b.field(EPOCH, epochNanos);
            AttributeRecord.writeArray(b, ATTRIBUTES, attributes, params);
            b.field(VALUE, value);
            b.endObject();
            return b;
        }
    }

    record HistogramPoint(
        long startEpochNanos,
        long epochNanos,
        List<AttributeRecord> attributes,
        double sum,
        long count,
        boolean hasMin,
        double min,
        boolean hasMax,
        double max,
        List<Double> boundaries,
        List<Long> counts
    ) implements PointRecord {

        static HistogramPoint from(HistogramPointData p) {
            return new HistogramPoint(
                p.getStartEpochNanos(),
                p.getEpochNanos(),
                AttributeRecord.fromAttributes(p.getAttributes()),
                p.getSum(),
                p.getCount(),
                p.hasMin(),
                p.getMin(),
                p.hasMax(),
                p.getMax(),
                p.getBoundaries(),
                p.getCounts()
            );
        }

        HistogramPointData to() {
            return HistogramPointData.create(
                startEpochNanos,
                epochNanos,
                AttributeRecord.toAttributes(attributes),
                sum,
                hasMin,
                min,
                hasMax,
                max,
                boundaries,
                counts
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
            b.startObject();
            b.field(KIND, KIND_HISTOGRAM);
            b.field(START, startEpochNanos);
            b.field(EPOCH, epochNanos);
            AttributeRecord.writeArray(b, ATTRIBUTES, attributes, params);
            b.field(SUM, sum);
            b.field(COUNT, count);
            b.field(HAS_MIN, hasMin);
            b.field(MIN, min);
            b.field(HAS_MAX, hasMax);
            b.field(MAX, max);
            b.field(BOUNDARIES, boundaries);
            b.field(COUNTS, counts);
            b.endObject();
            return b;
        }
    }

    /**
     * Hand-written dispatcher. Reads all fields into locals (because parser order isn't guaranteed),
     * then constructs the variant indicated by {@code kind}. Missing required fields propagate as
     * NPEs and are caught by the disk-buffer poisoned-file handler.
     */
    static PointRecord parse(XContentParser p) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, p.currentToken(), p);
        String kind = null;
        long startEpochNanos = 0;
        long epochNanos = 0;
        List<AttributeRecord> attributes = List.of();
        Number value = null;
        Double sum = null;
        Long count = null;
        Boolean hasMin = null;
        Double min = null;
        Boolean hasMax = null;
        Double max = null;
        List<Double> boundaries = null;
        List<Long> counts = null;

        while (p.nextToken() != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, p.currentToken(), p);
            String name = p.currentName();
            p.nextToken();
            switch (name) {
                case KIND -> kind = p.text();
                case START -> startEpochNanos = p.longValue();
                case EPOCH -> epochNanos = p.longValue();
                case ATTRIBUTES -> attributes = parseAttributes(p);
                case VALUE -> value = p.numberValue();
                case SUM -> sum = p.doubleValue();
                case COUNT -> count = p.longValue();
                case HAS_MIN -> hasMin = p.booleanValue();
                case MIN -> min = p.doubleValue();
                case HAS_MAX -> hasMax = p.booleanValue();
                case MAX -> max = p.doubleValue();
                case BOUNDARIES -> boundaries = parseDoubles(p);
                case COUNTS -> counts = parseLongs(p);
                default -> p.skipChildren();
            }
        }

        if (kind == null) {
            throw new IllegalArgumentException("required point field [" + KIND + "] is missing");
        }
        return switch (kind) {
            case KIND_LONG -> new LongPoint(startEpochNanos, epochNanos, attributes, value.longValue());
            case KIND_DOUBLE -> new DoublePoint(startEpochNanos, epochNanos, attributes, value.doubleValue());
            case KIND_HISTOGRAM -> new HistogramPoint(
                startEpochNanos,
                epochNanos,
                attributes,
                sum,
                count,
                hasMin,
                min,
                hasMax,
                max,
                boundaries,
                counts
            );
            default -> throw new IllegalArgumentException("unknown point kind [" + kind + "]");
        };
    }

    private static List<AttributeRecord> parseAttributes(XContentParser p) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, p.currentToken(), p);
        List<AttributeRecord> out = new ArrayList<>();
        while (p.nextToken() != XContentParser.Token.END_ARRAY) {
            out.add(AttributeRecord.PARSER.parse(p, null));
        }
        return out;
    }

    private static List<Double> parseDoubles(XContentParser p) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, p.currentToken(), p);
        List<Double> out = new ArrayList<>();
        while (p.nextToken() != XContentParser.Token.END_ARRAY) {
            out.add(p.doubleValue());
        }
        return out;
    }

    private static List<Long> parseLongs(XContentParser p) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, p.currentToken(), p);
        List<Long> out = new ArrayList<>();
        while (p.nextToken() != XContentParser.Token.END_ARRAY) {
            out.add(p.longValue());
        }
        return out;
    }
}
