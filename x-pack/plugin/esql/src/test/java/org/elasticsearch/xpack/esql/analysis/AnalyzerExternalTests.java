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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.VirtualAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
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
     * MATCH function can operate on external (non-index) fields via runtime lexical search.
     */
    public void testWithMatchFunctionAccepted() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        var plan = external().query("EXTERNAL \"" + S3_PATH + "\" | WHERE MATCH(first_name, \"foo\")");
        Project project = as(plan, Project.class);
        Limit limit = as(project.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(Match.class));
        assertThat(filter.child(), instanceOf(ExternalRelation.class));
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
     * Explicit {@code KEEP _file.path} surfaces the virtual column in the final plan output —
     * naming a virtual column by KEEP is the one way it reaches the result.
     */
    public void testKeepFileMetadataByNameSurfaces() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        var plan = external().query("EXTERNAL \"" + S3_PATH + "\" | KEEP `_file.path` | LIMIT 3");
        List<String> outputNames = plan.output().stream().map(Attribute::name).toList();
        assertEquals("explicit KEEP _file.path must surface it", List.of("_file.path"), outputNames);
    }

    /**
     * {@code DROP <data column>} resolves to a Project that carries every surviving column forward,
     * including the {@code _file.*} virtual columns the EXTERNAL shim made resolvable. That carry-
     * forward is NOT "the user kept it": no {@code _file.*} column may reach the final output.
     * This is the regression guard for the {@code EXTERNAL | DROP | LIMIT} leak.
     */
    public void testDropDoesNotSurfaceFileMetadata() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        var plan = external().query("EXTERNAL \"" + S3_PATH + "\" | DROP first_name | LIMIT 3");
        for (Attribute attr : plan.output()) {
            assertFalse("Virtual attribute " + attr.name() + " must not surface through DROP", attr instanceof VirtualAttribute);
            assertFalse(
                "_file.* column " + attr.name() + " must not surface through DROP",
                FileMetadataColumns.NAMES.contains(attr.name())
            );
        }
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

        // The EXTERNAL auto-attach shim injects every _file.* column EXCEPT _file.record_ref, which
        // is a FROM-only, request-driven column (it drives _id and forces the reader's row-position
        // channel). So KEEP _file* matches NAMES minus record_ref.
        int expectedShimColumns = FileMetadataColumns.NAMES.size() - 1;
        boolean foundFileMetadataProject = false;
        for (Project project : projects) {
            var fileAttrs = project.output().stream().filter(a -> a instanceof ExternalMetadataAttribute).toList();
            if (fileAttrs.size() == expectedShimColumns) {
                foundFileMetadataProject = true;
                for (Attribute attr : fileAttrs) {
                    assertTrue("Expected _file.* column but got: " + attr.name(), FileMetadataColumns.NAMES.contains(attr.name()));
                    assertNotEquals("record_ref is FROM-only, not auto-attached by EXTERNAL", FileMetadataColumns.RECORD_REF, attr.name());
                }
            }
        }
        assertTrue("No Project node found with the auto-attached _file.* columns", foundFileMetadataProject);
    }

    /**
     * Universal-rule binding: every standard metadata name in
     * {@link MetadataAttribute#ATTRIBUTES_MAP} resolves to an {@link ExternalMetadataAttribute} of
     * the registered type when listed in METADATA on an external dataset. Exercised by feeding a
     * pre-constructed {@link UnresolvedExternalRelation} into the analyzer because the EXTERNAL
     * command grammar does not carry a METADATA clause yet — the analyzer's
     * {@code ResolveExternalRelations} rule is the binding site under test, and accepts either
     * source the same way.
     */
    public void testStandardMetadataBindsOnExternalDataset() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        // Names taken from MetadataAttribute.ATTRIBUTES_MAP — kept literal so the test fails noisily
        // if a name is renamed in the registry (and the registry is the contract under test).
        List<String> names = List.of(
            "_id",
            MetadataAttribute.INDEX,
            "_version",
            MetadataAttribute.SCORE,
            "_source",
            "_ignored",
            "_index_mode",
            MetadataAttribute.TSID_FIELD,
            MetadataAttribute.SIZE
        );

        var leafOutput = externalLeafOutput(analyzeExternalWithMetadata(S3_PATH, employeesSchema(), names, "my_dataset"));

        for (String name : names) {
            Attribute attr = leafOutput.stream().filter(a -> a.name().equals(name)).findFirst().orElse(null);
            assertNotNull("metadata column [" + name + "] should bind on the ExternalRelation leaf", attr);
            assertThat("[" + name + "] binds to ExternalMetadataAttribute", attr, instanceOf(ExternalMetadataAttribute.class));
            DataType expected = MetadataAttribute.dataType(name);
            assertEquals("[" + name + "] type matches MetadataAttribute.ATTRIBUTES_MAP", expected, attr.dataType());
        }
    }

    /**
     * {@code _tier} (canonical name {@code DataTierFieldMapper.NAME}) is snapshot-only in the
     * standard metadata registry. The binding rule must mirror that: in non-snapshot builds, the
     * name is unknown, so requesting it via {@link #analyzeExternalWithMetadata} (which models it
     * as a plain {@link UnresolvedAttribute}, not a METADATA-clause pattern) fails verification
     * with the usual "Unknown column" diagnostic; in snapshot builds it binds normally.
     */
    public void testStandardMetadataTierSnapshotOnly() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        DataType registered = MetadataAttribute.dataType("_tier");
        boolean snapshotOnly = registered == null;

        if (snapshotOnly) {
            org.elasticsearch.xpack.esql.VerificationException e = expectThrows(
                org.elasticsearch.xpack.esql.VerificationException.class,
                () -> analyzeExternalWithMetadata(S3_PATH, employeesSchema(), List.of("_tier"), "my_dataset")
            );
            assertThat(e.getMessage(), containsString("Unknown column [_tier]"));
        } else {
            var leafOutput = externalLeafOutput(analyzeExternalWithMetadata(S3_PATH, employeesSchema(), List.of("_tier"), "my_dataset"));
            boolean bound = leafOutput.stream().anyMatch(a -> a.name().equals("_tier"));
            assertTrue("_tier must bind in snapshot builds", bound);
        }
    }

    /**
     * {@code _file.*} names continue to bind when requested via METADATA — the registry fallback
     * in {@code ResolveExternalRelations} looks up {@link FileMetadataColumns#COLUMNS} after the
     * standard-metadata map miss. Regression test for opt-in semantics.
     */
    public void testFileMetadataStillBindsViaMetadataClause() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        var leafOutput = externalLeafOutput(
            analyzeExternalWithMetadata(S3_PATH, employeesSchema(), List.of("_file.name", "_file.path"), "my_dataset")
        );

        Attribute nameAttr = leafOutput.stream().filter(a -> a.name().equals("_file.name")).findFirst().orElseThrow();
        Attribute pathAttr = leafOutput.stream().filter(a -> a.name().equals("_file.path")).findFirst().orElseThrow();

        assertThat(nameAttr, instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, nameAttr.dataType());
        assertThat(pathAttr, instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, pathAttr.dataType());
    }

    /**
     * Unknown METADATA names on the {@code FROM <dataset> METADATA …} path fire the same verifier
     * diagnostic as indexed FROM. The parser produces an
     * {@link org.elasticsearch.xpack.esql.core.expression.UnresolvedMetadataAttributeExpression};
     * the structural fix on {@link ExternalRelation} carries that expression forward so
     * {@code checkUnresolvedAttributes} surfaces it. Mirrors the indexed precedent at
     * {@code VerifierTests.testUnsupportedMetadata}.
     */
    public void testUnknownMetadataNameFiresVerifier() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        Expression tablePath = Literal.keyword(Source.EMPTY, S3_PATH);
        List<NamedExpression> metadataFields = List.of(
            new org.elasticsearch.xpack.esql.core.expression.UnresolvedMetadataAttributeExpression(Source.EMPTY, "_bogus")
        );
        UnresolvedExternalRelation unresolved = new UnresolvedExternalRelation(
            Source.EMPTY,
            tablePath,
            java.util.Map.of(),
            metadataFields,
            "my_dataset"
        );
        var analyzer = analyzer().externalSourceUnresolved(S3_PATH, employeesSchema());

        org.elasticsearch.xpack.esql.VerificationException e = expectThrows(
            org.elasticsearch.xpack.esql.VerificationException.class,
            () -> analyzer.buildAnalyzer().analyze(unresolved)
        );
        assertThat(e.getMessage(), containsString("Unresolved metadata pattern [_bogus]"));
    }

    /**
     * Returns the {@link ExternalRelation} leaf's output from an analyzed plan. The leaf's output
     * carries every name bound by {@code ResolveExternalRelations}, which is the binding contract
     * under test — the plan's top-level output may strip virtual attributes by design.
     */
    private static List<Attribute> externalLeafOutput(org.elasticsearch.xpack.esql.plan.logical.LogicalPlan analyzed) {
        var leaves = new ArrayList<ExternalRelation>();
        analyzed.forEachDown(ExternalRelation.class, leaves::add);
        assertThat("analyzed plan must contain exactly one ExternalRelation", leaves, hasSize(1));
        return leaves.get(0).output();
    }

    /**
     * Analyzes a dataset-style external relation built directly from the inputs, bypassing the
     * EXTERNAL command grammar (which has no METADATA clause). Returns the analyzed leaf so the
     * caller can inspect its output. Threading the {@code datasetName} mirrors what
     * {@code DatasetRewriter} produces on the {@code FROM <dataset>} path.
     */
    private static org.elasticsearch.xpack.esql.plan.logical.LogicalPlan analyzeExternalWithMetadata(
        String path,
        List<Attribute> schema,
        List<String> metadataNames,
        String datasetName
    ) {
        Expression tablePath = Literal.keyword(Source.EMPTY, path);
        List<NamedExpression> metadataFields = metadataNames.stream()
            .map(name -> (NamedExpression) new UnresolvedAttribute(Source.EMPTY, name))
            .toList();
        UnresolvedExternalRelation unresolved = new UnresolvedExternalRelation(
            Source.EMPTY,
            tablePath,
            java.util.Map.of(),
            metadataFields,
            datasetName
        );
        var analyzer = analyzer().externalSourceUnresolved(path, schema);
        return analyzer.buildAnalyzer().analyze(unresolved);
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
