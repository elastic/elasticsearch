/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk.serializer;

import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.GaugeData;
import io.opentelemetry.sdk.metrics.data.HistogramData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.SumData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Top-level wire representation of an OTel {@link MetricData} record.
 * <p>
 * Returns {@code null} (with a logged warning) for metric types that this codec does not support
 * ({@link MetricDataType#EXPONENTIAL_HISTOGRAM} and {@link MetricDataType#SUMMARY}) so the rest of a
 * batch survives both serialization and replay.
 */
record MetricRecord(
    String name,
    String description,
    String unit,
    MetricDataType type,
    ResourceRecord resource,
    ScopeRecord scope,
    Boolean isMonotonic,
    AggregationTemporality temporality,
    List<PointRecord> points
) implements ToXContentObject {

    private static final Logger logger = LogManager.getLogger(MetricRecord.class);

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField DESCRIPTION = new ParseField("description");
    private static final ParseField UNIT = new ParseField("unit");
    private static final ParseField TYPE = new ParseField("type");
    private static final ParseField RESOURCE = new ParseField("resource");
    private static final ParseField SCOPE = new ParseField("scope");
    private static final ParseField IS_MONOTONIC = new ParseField("is_monotonic");
    private static final ParseField TEMPORALITY = new ParseField("temporality");
    private static final ParseField POINTS = new ParseField("points");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<MetricRecord, Void> PARSER = new ConstructingObjectParser<>(
        "metric",
        true,
        args -> new MetricRecord(
            (String) args[0],
            (String) args[1],
            (String) args[2],
            (MetricDataType) args[3],
            (ResourceRecord) args[4],
            (ScopeRecord) args[5],
            (Boolean) args[6],
            (AggregationTemporality) args[7],
            (List<PointRecord>) args[8]
        )
    );
    static {
        PARSER.declareString(constructorArg(), NAME);
        PARSER.declareString(constructorArg(), DESCRIPTION);
        PARSER.declareString(constructorArg(), UNIT);
        PARSER.declareString(constructorArg(), MetricDataType::valueOf, TYPE);
        PARSER.declareObject(constructorArg(), ResourceRecord.PARSER, RESOURCE);
        PARSER.declareObject(constructorArg(), ScopeRecord.PARSER, SCOPE);
        PARSER.declareBoolean(optionalConstructorArg(), IS_MONOTONIC);
        PARSER.declareString(optionalConstructorArg(), AggregationTemporality::valueOf, TEMPORALITY);
        PARSER.declareField(constructorArg(), MetricRecord::parsePoints, POINTS, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    private static List<PointRecord> parsePoints(XContentParser p) throws IOException {
        List<PointRecord> points = new ArrayList<>();
        while (p.nextToken() != XContentParser.Token.END_ARRAY) {
            points.add(PointRecord.parse(p));
        }
        return points;
    }

    static MetricRecord fromMetricData(MetricData m) {
        Boolean isMonotonic = null;
        AggregationTemporality temporality = null;
        List<PointRecord> points;
        switch (m.getType()) {
            case LONG_GAUGE -> points = m.getLongGaugeData().getPoints().stream().<PointRecord>map(PointRecord.LongPoint::from).toList();
            case DOUBLE_GAUGE -> points = m.getDoubleGaugeData()
                .getPoints()
                .stream()
                .<PointRecord>map(PointRecord.DoublePoint::from)
                .toList();
            case LONG_SUM -> {
                SumData<LongPointData> sum = m.getLongSumData();
                isMonotonic = sum.isMonotonic();
                temporality = sum.getAggregationTemporality();
                points = sum.getPoints().stream().<PointRecord>map(PointRecord.LongPoint::from).toList();
            }
            case DOUBLE_SUM -> {
                SumData<DoublePointData> sum = m.getDoubleSumData();
                isMonotonic = sum.isMonotonic();
                temporality = sum.getAggregationTemporality();
                points = sum.getPoints().stream().<PointRecord>map(PointRecord.DoublePoint::from).toList();
            }
            case HISTOGRAM -> {
                HistogramData hist = m.getHistogramData();
                temporality = hist.getAggregationTemporality();
                points = hist.getPoints().stream().<PointRecord>map(PointRecord.HistogramPoint::from).toList();
            }
            default -> {
                logger.warn("dropping metric [{}] with unsupported type [{}] from disk buffer batch", m.getName(), m.getType());
                return null;
            }
        }
        return new MetricRecord(
            m.getName(),
            m.getDescription(),
            m.getUnit(),
            m.getType(),
            ResourceRecord.fromResource(m.getResource()),
            ScopeRecord.fromScope(m.getInstrumentationScopeInfo()),
            isMonotonic,
            temporality,
            points
        );
    }

    MetricData toMetricData() {
        Resource res = resource.toResource();
        var scopeInfo = scope.toScope();
        return switch (type) {
            case LONG_GAUGE -> ImmutableMetricData.createLongGauge(
                res,
                scopeInfo,
                name,
                description,
                unit,
                GaugeData.createLongGaugeData(points.stream().map(p -> ((PointRecord.LongPoint) p).to()).toList())
            );
            case DOUBLE_GAUGE -> ImmutableMetricData.createDoubleGauge(
                res,
                scopeInfo,
                name,
                description,
                unit,
                GaugeData.createDoubleGaugeData(points.stream().map(p -> ((PointRecord.DoublePoint) p).to()).toList())
            );
            case LONG_SUM -> ImmutableMetricData.createLongSum(
                res,
                scopeInfo,
                name,
                description,
                unit,
                SumData.createLongSumData(isMonotonic, temporality, points.stream().map(p -> ((PointRecord.LongPoint) p).to()).toList())
            );
            case DOUBLE_SUM -> ImmutableMetricData.createDoubleSum(
                res,
                scopeInfo,
                name,
                description,
                unit,
                SumData.createDoubleSumData(isMonotonic, temporality, points.stream().map(p -> ((PointRecord.DoublePoint) p).to()).toList())
            );
            case HISTOGRAM -> ImmutableMetricData.createDoubleHistogram(
                res,
                scopeInfo,
                name,
                description,
                unit,
                HistogramData.create(temporality, points.stream().map(p -> ((PointRecord.HistogramPoint) p).to()).toList())
            );
            default -> {
                logger.warn("dropping metric [{}] with unsupported type [{}] from replayed disk buffer batch", name, type);
                yield null;
            }
        };
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field(NAME.getPreferredName(), name);
        b.field(DESCRIPTION.getPreferredName(), description);
        b.field(UNIT.getPreferredName(), unit);
        b.field(TYPE.getPreferredName(), type.name());
        b.field(RESOURCE.getPreferredName(), resource);
        b.field(SCOPE.getPreferredName(), scope);
        if (isMonotonic != null) b.field(IS_MONOTONIC.getPreferredName(), isMonotonic);
        if (temporality != null) b.field(TEMPORALITY.getPreferredName(), temporality.name());
        b.startArray(POINTS.getPreferredName());
        for (PointRecord p : points) {
            p.toXContent(b, params);
        }
        b.endArray();
        b.endObject();
        return b;
    }
}
