/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.VirtualAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.hamcrest.Matcher;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for analysis of external datasets reached via {@code FROM <dataset>}. All such analyzer tests belong
 * here; {@code AnalyzerTests} must contain no external-dataset tests.
 * <p>
 * The {@code Analyzer} itself never parses {@code FROM <dataset>} — that rewrite happens earlier, in
 * {@code DatasetRewriter}, which turns the {@code UnresolvedRelation} for a registered dataset into the same
 * {@code UnresolvedExternalRelation} the (internal-only, snapshot-gated) {@code EXTERNAL} command produces. These
 * tests drive that same production pipeline: parse the query, run {@code DatasetRewriter.rewriteUnsecured} against a
 * single-dataset {@code ProjectMetadata}, then analyze — {@code EXTERNAL} itself is no longer part of the surface
 * these tests exercise; it remains only as the internal rewrite target.
 */
// @TestLogging(value = "org.elasticsearch.xpack.esql.analysis:TRACE", reason = "debug")
public class AnalyzerExternalTests extends ESTestCase {

    public static final String S3_PATH = "s3://bucket/data.parquet";

    /** Dataset name registered by {@link #analyzeDataset}/{@link #datasetError} for every {@code FROM <dataset>} query below. */
    public static final String DATASET_NAME = "my_dataset";

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testResolveExternalRelationPassesGenericFileList() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        var entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/f1.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f2.parquet"), 200, Instant.EPOCH)
        );
        var fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");

        List<Attribute> schema = List.of(referenceAttribute("id", LONG), referenceAttribute("name", KEYWORD));

        var testAnalyzer = analyzer().externalSourceResolution("s3://bucket/data/*.parquet", schema, fileList);
        var analyzed = analyzeDataset(testAnalyzer, "s3://bucket/data/*.parquet", "FROM " + DATASET_NAME + " | STATS count = COUNT(*)");

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
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        var analyzed = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " | STATS count = COUNT(*)");

        var externalRelations = new ArrayList<ExternalRelation>();
        analyzed.forEachDown(ExternalRelation.class, externalRelations::add);

        assertThat("Should have one ExternalRelation", externalRelations, hasSize(1));
        var externalRelation = externalRelations.get(0);

        assertFalse(externalRelation.fileList().isResolved());
        assertSame(FileList.UNRESOLVED, externalRelation.fileList());
    }

    /**
     * METRICS_INFO requires TS source command; an external dataset is rejected.
     */
    public void testWithMetricsInfoRejected() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        datasetError(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " | METRICS_INFO",
            containsString("METRICS_INFO can only be used with TS source command")
        );
    }

    /**
     * TS_INFO requires TS source command; an external dataset is rejected.
     */
    public void testWithTsInfoRejected() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        datasetError(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " | TS_INFO",
            containsString("TS_INFO can only be used with TS source command")
        );
    }

    /**
     * MATCH function can operate on external (non-index) fields via runtime lexical search.
     */
    public void testWithMatchFunctionAccepted() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " | WHERE MATCH(first_name, \"foo\")");
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(Match.class));
        assertThat(filter.child(), instanceOf(ExternalRelation.class));
    }

    /**
     * Match function requires field from index mapping; external-dataset fields are rejected, and the message names
     * the federated-source limitation.
     */
    public void testWithMatchPhraseFunctionRejected() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        datasetError(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " | WHERE MATCH_PHRASE(first_name, \"foo\")",
            containsString(
                "function cannot operate on [first_name], which is not a field from an index mapping "
                    + "(the source is a federated data source, not an index)"
            )
        );
    }

    /**
     * KQL function requires a Lucene index; an external (federated) dataset is rejected with a message naming the
     * dataset and the limitation, and suggesting the MATCH(field, ...) alternative, rather than a generic positional
     * error.
     */
    public void testWithKqlFunctionRejected() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        datasetError(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " | WHERE KQL(\"first_name: foo\")",
            containsString(
                "[KQL] function is not supported on federated data sources ["
                    + DATASET_NAME
                    + "]; it requires an index. Use MATCH(field, \"term\") for full-text search on non-indexed data."
            )
        );
    }

    /**
     * QSTR function requires a Lucene index; an external (federated) dataset is rejected with a message naming the
     * dataset and the limitation, and suggesting the MATCH(field, ...) alternative, rather than a generic positional
     * error.
     */
    public void testWithQstrFunctionRejected() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        datasetError(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " | WHERE QSTR(\"first_name: foo\")",
            containsString(
                "[QSTR] function is not supported on federated data sources ["
                    + DATASET_NAME
                    + "]; it requires an index. Use MATCH(field, \"term\") for full-text search on non-indexed data."
            )
        );
    }

    /**
     * KNN function requires vector field from index; an external dataset is rejected, and the message names the
     * federated-source limitation.
     */
    public void testWithKnnFunctionRejected() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        List<Attribute> schema = List.of(referenceAttribute("id", LONG), referenceAttribute("vector", DENSE_VECTOR));
        var testAnalyzer = analyzer().externalSourceUnresolved(S3_PATH, schema);

        datasetError(
            testAnalyzer,
            S3_PATH,
            "FROM " + DATASET_NAME + " | WHERE KNN(vector, [3, 100, 0])",
            containsString(
                "function cannot operate on [vector], which is not a field from an index mapping "
                    + "(the source is a federated data source, not an index)"
            )
        );
    }

    /**
     * MatchPhrase still names the federated-source limitation for a field that was renamed before reaching the
     * function - the federated-clause detection must follow the rename (by attribute identity) back to the
     * {@link ExternalRelation}, not just compare the field's current name against its output.
     */
    public void testWithMatchPhraseFunctionRejectedAfterRename() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        datasetError(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " | RENAME first_name AS fname | WHERE MATCH_PHRASE(fname, \"foo\")",
            containsString(
                "function cannot operate on [fname], which is not a field from an index mapping "
                    + "(the source is a federated data source, not an index)"
            )
        );
    }

    /**
     * MatchPhrase on a genuinely computed (EVAL-derived) field - even one built from a federated field, and even
     * after a subsequent rename - must not gain the federated-source clause: the value is no longer a direct
     * reference to the federated field, so the plain "not a field from an index mapping" message applies.
     */
    public void testWithMatchPhraseFunctionRejectedOnEvalDerivedField() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        datasetError(
            external(),
            S3_PATH,
            "FROM "
                + DATASET_NAME
                + " | EVAL full_name = CONCAT(first_name, last_name) | RENAME full_name AS content "
                + "| WHERE MATCH_PHRASE(content, \"foo\")",
            allOf(
                containsString("function cannot operate on [content], which is not a field from an index mapping"),
                not(containsString("federated"))
            )
        );
    }

    /**
     * {@code _file.name} resolves to an {@link ExternalMetadataAttribute} with type KEYWORD when requested via
     * {@code METADATA} — external metadata columns are request-driven, not auto-attached.
     * Verified via the {@link ExternalRelation} output (the plan's top-level output strips virtual columns by design).
     */
    public void testFileMetadataResolvesToExternalMetadataAttribute() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " METADATA _file.name | STATS c = COUNT_DISTINCT(`_file.name`)"
        );

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
     * Multiple {@code _file.*} columns requested via {@code METADATA} resolve with correct types in the external
     * relation schema.
     */
    public void testFileMetadataMultiColumnProjection() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(
            external(),
            S3_PATH,
            "FROM "
                + DATASET_NAME
                + " METADATA _file.name, _file.size, _file.modified"
                + " | STATS n = COUNT_DISTINCT(`_file.name`), s = MIN(`_file.size`), m = MAX(`_file.modified`)"
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
     * An unknown {@code _file.} column is rejected by the analyzer, with or without a {@code METADATA} clause.
     */
    public void testFileMetadataUnknownColumnRejected() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        datasetError(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " | KEEP _file.nonexistent",
            containsString("Unknown column [_file.nonexistent]")
        );
    }

    /**
     * {@code KEEP *} does not include virtual columns ({@code _file.*}), even when they are requested via
     * {@code METADATA}.
     */
    public void testFileMetadataExcludedFromStar() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " " + ALL_FILE_METADATA_CLAUSE + " | KEEP *");
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
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _file.path | KEEP `_file.path` | LIMIT 3");
        List<String> outputNames = plan.output().stream().map(Attribute::name).toList();
        assertEquals("explicit KEEP _file.path must surface it", List.of("_file.path"), outputNames);
    }

    /**
     * On the {@code FROM <dataset>} path, a {@code METADATA}-requested column surfaces
     * unconditionally, with no need for an explicit {@code KEEP} — unlike the (internal-only)
     * {@code EXTERNAL} command, whose shim auto-attaches the whole {@code _file.*} family and
     * therefore must hide it from default output until named. {@code DROP <unrelated column>}
     * resolves to a plain Project that carries every surviving column forward, and on the FROM
     * path that carry-forward legitimately reaches the final output.
     */
    public void testFileMetadataSurvivesDropOfUnrelatedColumn() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _file.path | DROP first_name | LIMIT 3");
        List<String> outputNames = plan.output().stream().map(Attribute::name).toList();
        assertThat("METADATA _file.path must survive DROP of an unrelated column", outputNames, hasItem("_file.path"));
    }

    /**
     * {@code KEEP _file*} pattern resolves every {@code _file.*} column requested via {@code METADATA}.
     * Verified by piping into STATS (since the final plan output strips virtual columns).
     */
    public void testFileMetadataExplicitPatternMatches() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " " + ALL_FILE_METADATA_CLAUSE + " | KEEP _file* | STATS c = COUNT(*)"
        );

        var projects = new ArrayList<Project>();
        plan.forEachDown(Project.class, projects::add);

        // ALL_FILE_METADATA_CLAUSE requests every _file.* column EXCEPT _file.record_ref, which is a request-driven
        // column that must be named explicitly (it drives _id and forces the reader's row-position channel), so
        // KEEP _file* matches NAMES minus record_ref.
        int expectedMetadataColumns = FileMetadataColumns.NAMES.size() - 1;
        boolean foundFileMetadataProject = false;
        for (Project project : projects) {
            var fileAttrs = project.output().stream().filter(a -> a instanceof ExternalMetadataAttribute).toList();
            if (fileAttrs.size() == expectedMetadataColumns) {
                foundFileMetadataProject = true;
                for (Attribute attr : fileAttrs) {
                    assertTrue("Expected _file.* column but got: " + attr.name(), FileMetadataColumns.NAMES.contains(attr.name()));
                    assertNotEquals("record_ref must be requested explicitly", FileMetadataColumns.RECORD_REF, attr.name());
                }
            }
        }
        assertTrue("No Project node found with the requested _file.* columns", foundFileMetadataProject);
    }

    /**
     * Universal-rule binding: every standard metadata name in
     * {@link MetadataAttribute#ATTRIBUTES_MAP} resolves to an {@link ExternalMetadataAttribute} of
     * the registered type when listed in {@code METADATA} on an external dataset.
     */
    public void testStandardMetadataBindsOnExternalDataset() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

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

        var leafOutput = externalLeafOutput(
            analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA " + String.join(", ", names))
        );

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
     * name is unresolved, so requesting it via {@code METADATA _tier} fails verification with the
     * usual "Unresolved metadata pattern" diagnostic; in snapshot builds it binds normally.
     */
    public void testStandardMetadataTierSnapshotOnly() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        DataType registered = MetadataAttribute.dataType("_tier");
        boolean snapshotOnly = registered == null;
        String query = "FROM " + DATASET_NAME + " METADATA _tier";

        if (snapshotOnly) {
            datasetError(external(), S3_PATH, query, containsString("Unresolved metadata pattern [_tier]"));
        } else {
            var leafOutput = externalLeafOutput(analyzeDataset(external(), S3_PATH, query));
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
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var leafOutput = externalLeafOutput(
            analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _file.name, _file.path")
        );

        Attribute nameAttr = leafOutput.stream().filter(a -> a.name().equals("_file.name")).findFirst().orElseThrow();
        Attribute pathAttr = leafOutput.stream().filter(a -> a.name().equals("_file.path")).findFirst().orElseThrow();

        assertThat(nameAttr, instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, nameAttr.dataType());
        assertThat(pathAttr, instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, pathAttr.dataType());
    }

    /**
     * Unknown METADATA names on {@code FROM <dataset> METADATA …} fire the same verifier
     * diagnostic as indexed FROM. Mirrors the indexed precedent at
     * {@code VerifierTests.testUnsupportedMetadata}.
     */
    public void testUnknownMetadataNameFiresVerifier() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        datasetError(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " METADATA _bogus",
            containsString("Unresolved metadata pattern [_bogus]")
        );
    }

    /**
     * Returns the {@link ExternalRelation} leaf's output from an analyzed plan. The leaf's output
     * carries every name bound by {@code ResolveExternalRelations}, which is the binding contract
     * under test — the plan's top-level output may strip virtual attributes by design.
     */
    private static List<Attribute> externalLeafOutput(LogicalPlan analyzed) {
        var leaves = new ArrayList<ExternalRelation>();
        analyzed.forEachDown(ExternalRelation.class, leaves::add);
        assertThat("analyzed plan must contain exactly one ExternalRelation", leaves, hasSize(1));
        return leaves.get(0).output();
    }

    /**
     * Registers a single dataset ({@link #DATASET_NAME} → {@code resource}) in a {@link ProjectMetadata}, parses
     * {@code query}, rewrites {@code FROM <dataset>} into the {@code UnresolvedExternalRelation} that
     * {@code DatasetRewriter} produces in production, and analyzes the result with {@code testAnalyzer}.
     */
    private static LogicalPlan analyzeDataset(TestAnalyzer testAnalyzer, String resource, String query) {
        LogicalPlan rewritten = DatasetRewriter.rewriteUnsecured(
            TEST_PARSER.parseQuery(query),
            datasetProject(resource),
            TestIndexNameExpressionResolver.newInstance()
        );
        return testAnalyzer.buildAnalyzer().analyze(rewritten);
    }

    /**
     * Like {@link #analyzeDataset} but for expected analysis errors — mirrors {@link TestAnalyzer#error}.
     */
    private static String datasetError(TestAnalyzer testAnalyzer, String resource, String query, Matcher<String> messageMatcher) {
        VerificationException e = expectThrows(VerificationException.class, () -> analyzeDataset(testAnalyzer, resource, query));
        assertThat(e.getMessage(), messageMatcher);
        return e.getMessage();
    }

    /** A single-dataset {@link ProjectMetadata}: {@link #DATASET_NAME} pointing at {@code resource}. */
    private static ProjectMetadata datasetProject(String resource) {
        DataSource dataSource = new DataSource("ds_source", "test", null, Map.of());
        Dataset dataset = new Dataset(DATASET_NAME, new DataSourceReference("ds_source"), resource, null, Map.of());
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("ds_source", dataSource)))
            .datasets(Map.of(DATASET_NAME, dataset))
            .build();
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

    /** Every {@code _file.*} name except {@code _file.record_ref}, which is request-driven and must be named explicitly. */
    private static final String ALL_FILE_METADATA_CLAUSE = "METADATA _file.path, _file.name, _file.directory, _file.size, "
        + "_file.modified";
}
