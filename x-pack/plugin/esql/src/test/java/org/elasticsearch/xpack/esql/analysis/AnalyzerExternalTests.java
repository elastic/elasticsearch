/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.FileSet;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.junit.BeforeClass;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultEnrichResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultInferenceResolution;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

/**
 * Unit tests for EXTERNAL command analysis.
 * All EXTERNAL-related analyzer tests belong here; AnalyzerTests must contain no EXTERNAL tests.
 */
// @TestLogging(value = "org.elasticsearch.xpack.esql.analysis:TRACE", reason = "debug")
public class AnalyzerExternalTests extends ESTestCase {

    private static final String S3_PATH = "s3://bucket/data.parquet";

    /**
     * Built in {@link #initSharedExternalAnalyzer()}; not in a static field initializer so
     * {@link EsqlTestUtils#fieldAttribute} (random ids) runs under RandomizedRunner context.
     */
    private static Analyzer sharedExternalAnalyzer;

    @BeforeClass
    public static void initSharedExternalAnalyzer() {
        sharedExternalAnalyzer = externalAnalyzer(externalResolution(S3_PATH, employeesSchema()));
    }

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

        List<Attribute> schema = List.of(fieldAttribute("id", LONG), fieldAttribute("name", KEYWORD));

        var externalResolution = externalResolution("s3://bucket/data/*.parquet", schema, fileSet);
        var testAnalyzer = externalAnalyzer(externalResolution);

        var plan = TEST_PARSER.parseQuery("EXTERNAL \"s3://bucket/data/*.parquet\" | STATS count = COUNT(*)");
        var analyzed = testAnalyzer.analyze(plan);

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
        var plan = TEST_PARSER.parseQuery("EXTERNAL \"" + S3_PATH + "\" | STATS count = COUNT(*)");
        var analyzed = sharedExternalAnalyzer.analyze(plan);

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
    public void testExternalWithMetricsInfoRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> sharedExternalAnalyzer.analyze(TEST_PARSER.parseQuery("EXTERNAL \"" + S3_PATH + "\" | METRICS_INFO"))
        );
        assertThat(e.getMessage(), containsString("METRICS_INFO can only be used with TS source command"));
    }

    /**
     * TS_INFO requires TS source command; EXTERNAL is rejected.
     */
    public void testExternalWithTsInfoRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> sharedExternalAnalyzer.analyze(TEST_PARSER.parseQuery("EXTERNAL \"" + S3_PATH + "\" | TS_INFO"))
        );
        assertThat(e.getMessage(), containsString("TS_INFO can only be used with TS source command"));
    }

    /**
     * Match function requires field from index mapping; EXTERNAL fields are rejected.
     */
    @AwaitsFix(bugUrl = "TODO")
    public void testExternalWithMatchFunctionRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> sharedExternalAnalyzer.analyze(TEST_PARSER.parseQuery("EXTERNAL \"" + S3_PATH + "\" | WHERE MATCH(first_name, \"foo\")"))
        );
        assertThat(e.getMessage(), containsString("cannot operate on"));
        assertThat(e.getMessage(), containsString("not a field from an index mapping"));
    }

    /**
     * KQL function requires index context; EXTERNAL is rejected.
     */
    @AwaitsFix(bugUrl = "TODO")
    public void testExternalWithKqlFunctionRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> sharedExternalAnalyzer.analyze(TEST_PARSER.parseQuery("EXTERNAL \"" + S3_PATH + "\" | WHERE kql(\"first_name: foo\")"))
        );
        assertThat(e.getMessage(), containsString("cannot be used after"));
        assertThat(e.getMessage(), containsString("EXTERNAL"));
    }

    /**
     * KNN function requires vector field from index; EXTERNAL is rejected.
     */
    @AwaitsFix(bugUrl = "TODO")
    public void testExternalWithKnnFunctionRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        List<Attribute> schema = List.of(fieldAttribute("id", LONG), fieldAttribute("vector", DENSE_VECTOR));
        var resolution = externalResolution(S3_PATH, schema);
        var analyzer = externalAnalyzer(resolution);

        Throwable e = expectThrows(
            Throwable.class,
            () -> analyzer.analyze(TEST_PARSER.parseQuery("EXTERNAL \"" + S3_PATH + "\" | WHERE knn(\"vector\", 3, 100)"))
        );
        assertThat(e.getMessage(), anyOf(containsString("cannot operate on"), containsString("KNN"), containsString("knn")));
    }

    /**
     * Creates an analyzer context with external source resolution for the given path and schema.
     */
    private static Analyzer externalAnalyzer(ExternalSourceResolution externalResolution) {
        var context = new AnalyzerContext(
            EsqlTestUtils.TEST_CFG,
            TEST_FUNCTION_REGISTRY,
            null,
            Map.of(),
            Map.of(),
            defaultEnrichResolution(),
            defaultInferenceResolution(),
            externalResolution,
            TransportVersion.current(),
            QuerySettings.UNMAPPED_FIELDS.defaultValue()
        );
        return new Analyzer(context, TEST_VERIFIER);
    }

    /**
     * Builds external resolution for a path with the given schema.
     */
    private static ExternalSourceResolution externalResolution(String path, List<Attribute> schema) {
        return externalResolution(path, schema, FileSet.UNRESOLVED);
    }

    private static ExternalSourceResolution externalResolution(String path, List<Attribute> schema, FileSet fileSet) {
        var metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return path;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        };
        var resolvedSource = new ExternalSourceResolution.ResolvedSource(metadata, fileSet);
        return new ExternalSourceResolution(Map.of(path, resolvedSource));
    }

    private static List<Attribute> employeesSchema() {
        return List.of(
            fieldAttribute("emp_no", LONG),
            fieldAttribute("first_name", KEYWORD),
            fieldAttribute("last_name", KEYWORD),
            fieldAttribute("languages", INTEGER)
        );
    }
}
