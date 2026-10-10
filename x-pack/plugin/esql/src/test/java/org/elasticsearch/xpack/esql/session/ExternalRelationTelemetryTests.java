/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The plan traversal that gives a query its storage-type and format labels, and the path from those labels to the
 * by-source external-source query metrics. A union over two sources stands in for a multi-source query such as a join or a fork: both
 * relations must reach the telemetry, so a query that reads them is labelled {@code mixed}.
 */
public class ExternalRelationTelemetryTests extends ESTestCase {

    private static ExternalRelation relation(String location, String format) {
        Attribute year = new ReferenceAttribute(Source.EMPTY, "year", DataType.INTEGER);
        List<Attribute> output = List.of(year);
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, format, location, null, null, Map.of(), Map.of());
        return new ExternalRelation(Source.EMPTY, location, metadata, output, FileList.UNRESOLVED, Map.of());
    }

    public void testSingleRelationLabelsItsOwnStorageTypeAndFormat() {
        PlanTelemetry telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
        EsqlSession.recordExternalRelations(relation("s3://bucket/data.parquet", "parquet"), telemetry);

        assertThat(telemetry.externalStorageType(), equalTo("s3"));
        assertThat(telemetry.externalFormat(), equalTo("parquet"));
    }

    public void testEveryRelationOfAMultiSourcePlanIsRecorded() {
        LogicalPlan plan = new UnionAll(
            Source.EMPTY,
            List.of(relation("s3://bucket/data.parquet", "parquet"), relation("file:///tmp/local.csv", "csv")),
            List.of()
        );
        PlanTelemetry telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
        EsqlSession.recordExternalRelations(plan, telemetry);

        // Both sources were seen, so neither dimension can collapse to a single value.
        assertThat(telemetry.externalStorageType(), equalTo(ExternalSourceMetrics.MIXED));
        assertThat(telemetry.externalFormat(), equalTo(ExternalSourceMetrics.MIXED));
    }

    /** The mixed labels of a multi-source query reach the query metrics, on the total and on the duration. */
    public void testMixedLabelsOfAMultiSourceQueryReachTheMetrics() {
        LogicalPlan plan = new UnionAll(
            Source.EMPTY,
            List.of(relation("s3://bucket/data.parquet", "parquet"), relation("file:///tmp/local.csv", "csv")),
            List.of()
        );
        PlanTelemetry telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
        EsqlSession.recordExternalRelations(plan, telemetry);

        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);
        metrics.recordQuery(
            new ExternalSourceMetrics.QueryLabels(
                ExternalSourceMetrics.CLIENT_NONE,
                telemetry.externalStorageType(),
                telemetry.externalFormat()
            ),
            ExternalSourceMetrics.OUTCOME_SUCCESS,
            5L,
            false,
            null,
            null
        );

        Measurement total = single(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_BY_SOURCE_TOTAL);
        assertThat(total.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("mixed"));
        assertThat(total.attributes().get(ExternalSourceMetrics.FORMAT_ATTRIBUTE), equalTo("mixed"));
        Measurement duration = single(registry, InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.QUERY_BY_SOURCE_DURATION);
        assertThat(duration.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("mixed"));
        assertThat(duration.attributes().get(ExternalSourceMetrics.FORMAT_ATTRIBUTE), equalTo("mixed"));
    }

    private static Measurement single(RecordingMeterRegistry registry, InstrumentType type, String name) {
        List<Measurement> measurements = registry.getRecorder().getMeasurements(type, name);
        assertThat(measurements, hasSize(1));
        return measurements.get(0);
    }
}
