/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;

import static org.hamcrest.Matchers.equalTo;

/**
 * The external-source labels of a query: one storage type and one format when every external relation of the analyzed plan
 * agrees, {@code mixed} when they differ, and nothing when the query read no external source.
 */
public class PlanTelemetryExternalSourceTests extends ESTestCase {

    public void testNoExternalRelationGivesNoLabels() {
        PlanTelemetry telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
        assertNull(telemetry.externalStorageType());
        assertNull(telemetry.externalFormat());
    }

    public void testRelationsThatAgreeGiveTheirOwnLabels() {
        PlanTelemetry telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
        telemetry.externalRelation("s3", "parquet");
        telemetry.externalRelation("s3", "parquet");
        assertThat(telemetry.externalStorageType(), equalTo("s3"));
        assertThat(telemetry.externalFormat(), equalTo("parquet"));
    }

    public void testRelationsThatDisagreeOnBothDimensionsAreMixed() {
        PlanTelemetry telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
        telemetry.externalRelation("s3", "parquet");
        telemetry.externalRelation("local", "csv");
        assertThat(telemetry.externalStorageType(), equalTo(ExternalSourceMetrics.MIXED));
        assertThat(telemetry.externalFormat(), equalTo(ExternalSourceMetrics.MIXED));
    }

    /** One storage type across two formats is mixed on the format only, so the type label is still a single value. */
    public void testOneStorageTypeAcrossTwoFormatsIsMixedOnlyOnFormat() {
        PlanTelemetry telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
        telemetry.externalRelation("s3", "parquet");
        telemetry.externalRelation("s3", "csv");
        assertThat(telemetry.externalStorageType(), equalTo("s3"));
        assertThat(telemetry.externalFormat(), equalTo(ExternalSourceMetrics.MIXED));
    }
}
