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
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.VirtualAttribute;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

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

    public void testResolveExternalRelationPassesGenericFileList() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        var entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/f1.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f2.parquet"), 200, Instant.EPOCH)
        );
        var fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");

        List<Attribute> schema = List.of(referenceAttribute("id", LONG), referenceAttribute("name", KEYWORD));

        var analyzer = analyzer().externalSourceResolution("s3://bucket/data/*.parquet", schema, fileList);
        var analyzed = analyzer.query("EXTERNAL \"s3://bucket/data/*.parquet\" | STATS count = COUNT(*)");

        var externalRelations = new ArrayList<ExternalRelation>();
        analyzed.forEachDown(ExternalRelation.class, externalRelations::add);

        assertThat("Should have one ExternalRelation", externalRelations, hasSize(1));
        var externalRelation = externalRelations.get(0);

        assertSame(fileList, externalRelation.fileList());
        assertTrue(externalRelation.fileList().isResolved());
        assertEquals(2, externalRelation.fileList().fileCount());
        assertEquals("s3://bucket/data/*.parquet", externalRelation.fileList().originalPattern());
    }

    public void testResolveExternalRelationUnresolvedGenericFileList() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        var testAnalyzer = external();
        var analyzed = testAnalyzer.query("EXTERNAL \"" + S3_PATH + "\" | STATS count = COUNT(*)");

        var externalRelations = new ArrayList<ExternalRelation>();
        analyzed.forEachDown(ExternalRelation.class, externalRelations::add);

        assertThat("Should have one ExternalRelation", externalRelations, hasSize(1));
        var externalRelation = externalRelations.get(0);

        assertFalse(externalRelation.fileList().isResolved());
        assertSame(FileList.UNRESOLVED, externalRelation.fileList());
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
     * KQL function requires index context; EXTERNAL is rejected.
     */
    public void testWithKqlFunctionRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        external().error(
            "EXTERNAL \"" + S3_PATH + "\" | WHERE KQL(\"first_name: foo\")",
            containsString("[KQL] function cannot be used after [EXTERNAL \"" + S3_PATH + "\"]")
        );
    }

    /**
     * QSTR function requires index context; EXTERNAL is rejected.
     */
    public void testWithQstrFunctionRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        external().error(
            "EXTERNAL \"" + S3_PATH + "\" | WHERE QSTR(\"first_name: foo\")",
            containsString("[QSTR] function cannot be used after [EXTERNAL \"" + S3_PATH + "\"]")
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

    /**
     * {@code _file.name} resolves to an {@link ExternalMetadataAttribute} with type KEYWORD.
     * Verified via the {@link ExternalRelation} output (the plan's top-level output strips virtual columns by design).
     */
    public void testFileMetadataResolvesToExternalMetadataAttribute() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        var plan = external().query("EXTERNAL \"" + S3_PATH + "\" | STATS c = COUNT_DISTINCT(`_file.name`)");

        var externalRelations = new ArrayList<ExternalRelation>();
        plan.forEachDown(ExternalRelation.class, externalRelations::add);
        assertThat(externalRelations, hasSize(1));

        Attribute fileNameAttr = externalRelations.get(0)
            .output()
            .stream()
            .filter(a -> a.name().equals("_file.name"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("_file.name not found in ExternalRelation output"));

        assertThat(fileNameAttr, instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, fileNameAttr.dataType());
    }

    /**
     * Multiple {@code _file.*} columns resolve with correct types in the external relation schema.
     */
    public void testFileMetadataMultiColumnProjection() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        var plan = external().query(
            "EXTERNAL \"" + S3_PATH + "\" | STATS n = COUNT_DISTINCT(`_file.name`), " + "s = MIN(`_file.size`), m = MAX(`_file.modified`)"
        );

        var externalRelations = new ArrayList<ExternalRelation>();
        plan.forEachDown(ExternalRelation.class, externalRelations::add);
        assertThat(externalRelations, hasSize(1));

        var output = externalRelations.get(0).output();
        Attribute nameAttr = output.stream().filter(a -> a.name().equals("_file.name")).findFirst().orElseThrow();
        Attribute sizeAttr = output.stream().filter(a -> a.name().equals("_file.size")).findFirst().orElseThrow();
        Attribute modifiedAttr = output.stream().filter(a -> a.name().equals("_file.modified")).findFirst().orElseThrow();

        assertThat(nameAttr, instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, nameAttr.dataType());
        assertThat(sizeAttr, instanceOf(ExternalMetadataAttribute.class));
        assertEquals(LONG, sizeAttr.dataType());
        assertThat(modifiedAttr, instanceOf(ExternalMetadataAttribute.class));
        assertEquals(DATETIME, modifiedAttr.dataType());
    }

    /**
     * An unknown {@code _file.} column is rejected by the analyzer.
     */
    public void testFileMetadataUnknownColumnRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        external().error("EXTERNAL \"" + S3_PATH + "\" | KEEP _file.nonexistent", containsString("Unknown column [_file.nonexistent]"));
    }

    /**
     * {@code KEEP *} does not include virtual columns ({@code _file.*}).
     */
    public void testFileMetadataExcludedFromStar() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        var plan = external().query("EXTERNAL \"" + S3_PATH + "\" | KEEP *");
        for (Attribute attr : plan.output()) {
            assertFalse("Virtual attribute " + attr.name() + " should not appear in KEEP * output", attr instanceof VirtualAttribute);
        }
        assertEquals(employeesSchema().size(), plan.output().size());
    }

    /**
     * {@code KEEP _file*} pattern resolves all five {@code _file.*} columns.
     * Verified by piping into STATS (since the final plan output strips virtual columns).
     */
    public void testFileMetadataExplicitPatternMatches() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        var plan = external().query("EXTERNAL \"" + S3_PATH + "\" | KEEP _file* | STATS c = COUNT(*)");

        var projects = new ArrayList<Project>();
        plan.forEachDown(Project.class, projects::add);

        boolean foundFileMetadataProject = false;
        for (Project project : projects) {
            var fileAttrs = project.output().stream().filter(a -> a instanceof ExternalMetadataAttribute).toList();
            if (fileAttrs.size() == FileMetadataColumns.NAMES.size()) {
                foundFileMetadataProject = true;
                for (Attribute attr : fileAttrs) {
                    assertTrue("Expected _file.* column but got: " + attr.name(), FileMetadataColumns.NAMES.contains(attr.name()));
                }
            }
        }
        assertTrue("No Project node found with all 5 _file.* columns", foundFileMetadataProject);
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
