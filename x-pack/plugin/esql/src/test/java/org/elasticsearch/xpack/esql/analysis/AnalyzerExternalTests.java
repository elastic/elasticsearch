/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.FileSet;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

/**
 * Unit tests for EXTERNAL command analysis.
 * All EXTERNAL-related analyzer tests belong here; AnalyzerTests must contain no EXTERNAL tests.
 */
// @TestLogging(value = "org.elasticsearch.xpack.esql.analysis:TRACE", reason = "debug")
public class AnalyzerExternalTests extends ESTestCase {

    public static final String S3_PATH = "s3://bucket/data.parquet";

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testResolveExternalRelationPassesFileSet() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        var entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/f1.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f2.parquet"), 200, Instant.EPOCH)
        );
        var fileSet = new FileSet(entries, "s3://bucket/data/*.parquet");

        List<Attribute> schema = List.of(referenceAttribute("id", LONG), referenceAttribute("name", KEYWORD));

        var analyzer = analyzer().externalSourceResolution("s3://bucket/data/*.parquet", schema, fileSet);
        var analyzed = analyzer.query("EXTERNAL \"s3://bucket/data/*.parquet\" | STATS count = COUNT(*)");

        var externalRelations = new ArrayList<ExternalRelation>();
        analyzed.forEachDown(ExternalRelation.class, externalRelations::add);

        assertThat("Should have one ExternalRelation", externalRelations, hasSize(1));
        var externalRelation = externalRelations.get(0);

        assertSame(fileSet, externalRelation.fileSet());
        assertTrue(externalRelation.fileSet().isResolved());
        assertEquals(2, externalRelation.fileSet().size());
        assertEquals("s3://bucket/data/*.parquet", externalRelation.fileSet().originalPattern());
    }

    public void testResolveExternalRelationUnresolvedFileSet() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        var testAnalyzer = external();
        var analyzed = testAnalyzer.query("EXTERNAL \"" + S3_PATH + "\" | STATS count = COUNT(*)");

        var externalRelations = new ArrayList<ExternalRelation>();
        analyzed.forEachDown(ExternalRelation.class, externalRelations::add);

        assertThat("Should have one ExternalRelation", externalRelations, hasSize(1));
        var externalRelation = externalRelations.get(0);

        assertTrue(externalRelation.fileSet().isUnresolved());
        assertSame(FileSet.UNRESOLVED, externalRelation.fileSet());
    }

    /**
     * METRICS_INFO requires TS source command; EXTERNAL is rejected.
     */
    public void testWithMetricsInfoRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        external().error(
            "EXTERNAL \"" + S3_PATH + "\" | METRICS_INFO",
            containsString("METRICS_INFO can only be used with TS source command")
        );
    }

    /**
     * TS_INFO requires TS source command; EXTERNAL is rejected.
     */
    public void testWithTsInfoRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        external().error("EXTERNAL \"" + S3_PATH + "\" | TS_INFO", containsString("TS_INFO can only be used with TS source command"));
    }

    /**
     * Match function requires field from index mapping; EXTERNAL fields are rejected.
     */
    public void testWithMatchFunctionRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        external().error(
            "EXTERNAL \"" + S3_PATH + "\" | WHERE MATCH(first_name, \"foo\")",
            containsString("function cannot operate on [first_name], which is not a field from an index mapping")
        );
    }

    /**
     * Match function requires field from index mapping; EXTERNAL fields are rejected.
     */
    public void testWithMatchPhraseFunctionRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        external().error(
            "EXTERNAL \"" + S3_PATH + "\" | WHERE MATCH_PHRASE(first_name, \"foo\")",
            containsString("function cannot operate on [first_name], which is not a field from an index mapping")
        );
    }

    /**
     * Match function requires field from index mapping; EXTERNAL fields are rejected.
     */
    public void testWithMultiMatchFunctionRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        external().error(
            "EXTERNAL \"" + S3_PATH + "\" | WHERE MULTI_MATCH(\"foo\", first_name, last_name)",
            allOf(
                containsString("function cannot operate on [first_name], which is not a field from an index mapping"),
                containsString("function cannot operate on [last_name], which is not a field from an index mapping")
            )
        );
    }

    /**
     * KQL function requires index context; EXTERNAL is rejected.
     */
    public void testWithKqlFunctionRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        external().error(
            "EXTERNAL \"" + S3_PATH + "\" | WHERE KQL(\"first_name: foo\")",
            containsString("function cannot be used after EXTERNAL")
        );
    }

    /**
     * QSTR function requires index context; EXTERNAL is rejected.
     */
    public void testWithQstrFunctionRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        external().error(
            "EXTERNAL \"" + S3_PATH + "\" | WHERE QSTR(\"first_name: foo\")",
            containsString("function cannot be used after EXTERNAL")
        );
    }

    /**
     * KNN function requires vector field from index; EXTERNAL is rejected.
     */
    public void testWithKnnFunctionRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        List<Attribute> schema = List.of(referenceAttribute("id", LONG), referenceAttribute("vector", DENSE_VECTOR));
        var testAnalyzer = analyzer().externalSourceUnresolved(S3_PATH, schema);

        testAnalyzer.error(
            "EXTERNAL \"" + S3_PATH + "\" | WHERE KNN(vector, [3, 100, 0])",
            containsString("function cannot operate on [vector], which is not a field from an index mapping")
        );
    }

    public static TestAnalyzer external() {
        return analyzer().externalSourceUnresolved(S3_PATH, employeesSchema());
    }

    private static List<Attribute> employeesSchema() {
        return List.of(
            referenceAttribute("emp_no", LONG),
            referenceAttribute("first_name", KEYWORD),
            referenceAttribute("last_name", KEYWORD),
            referenceAttribute("languages", INTEGER)
        );
    }
}
