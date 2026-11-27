/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSpec;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.NestedColumns;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.hamcrest.Matcher;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.externalResolutionFor;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.rewriteDatasetsUnsecured;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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
 * tests drive that same production pipeline: parse the query, run {@code EsqlTestUtils.rewriteDatasetsUnsecured} against a
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

        var testAnalyzer = analyzer().externalSourceResolution(
            datasetProject("s3://bucket/data/*.parquet"),
            "s3://bucket/data/*.parquet",
            schema,
            fileList
        );
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
     * MATCH_PHRASE can operate on external (non-index) keyword fields via runtime search.
     */
    public void testWithMatchPhraseFunction() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " | WHERE MATCH_PHRASE(first_name, \"foo\")");
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(MatchPhrase.class));
        assertThat(filter.child(), instanceOf(ExternalRelation.class));
    }

    public void testScoreAggregationWithFullTextFilterRejected() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        String from = "FROM " + DATASET_NAME + " METADATA _score ";
        String message = "cannot use _score aggregations with a WHERE filter in a STATS command";
        datasetError(external(), S3_PATH, from + "| STATS c = MAX(_score) WHERE MATCH(first_name, \"foo\")", containsString(message));
        datasetError(
            external(),
            S3_PATH,
            from + "| STATS c = WEIGHTED_AVG(emp_no, _score) WHERE MATCH(first_name, \"foo\")",
            containsString(message)
        );
        // Filtering before the STATS is fine: the aggregation sees the score the match contributed.
        analyzeDataset(external(), S3_PATH, from + "| WHERE MATCH(first_name, \"foo\") | STATS c = MAX(_score)");
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
     * KNN function can operate on dense_vector fields in external dataset via runtime search
     */
    public void testWithKnnFunction() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        List<Attribute> schema = List.of(referenceAttribute("id", LONG), referenceAttribute("vector", DENSE_VECTOR));
        var testAnalyzer = analyzer().externalSourceUnresolved(datasetProject(S3_PATH), S3_PATH, schema);

        analyzeDataset(testAnalyzer, S3_PATH, "FROM " + DATASET_NAME + " | WHERE KNN(vector, [3, 100, 0])");
    }

    /**
     * MATCH_PHRASE runtime search also accepts a renamed external field.
     */
    public void testWithMatchPhraseFunctionAfterRename() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " | RENAME first_name AS fname | WHERE MATCH_PHRASE(fname, \"foo\")");
    }

    /**
     * MATCH_PHRASE runtime search accepts EVAL-derived keyword fields, even ones built from a federated field and
     * renamed afterwards.
     */
    public void testWithMatchPhraseFunctionOnEvalDerivedField() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        analyzeDataset(
            external(),
            S3_PATH,
            "FROM "
                + DATASET_NAME
                + " | EVAL full_name = CONCAT(first_name, last_name) | RENAME full_name AS content "
                + "| WHERE MATCH_PHRASE(content, \"foo\")"
        );
    }

    /**
     * {@code _file.name} resolves to an {@link ExternalMetadataAttribute} with type KEYWORD when requested via
     * {@code METADATA}. External metadata columns are request-driven, not auto-attached. Verified via the
     * {@link ExternalRelation} leaf output.
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

    public void testMatchOnFileMetadata() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " METADATA _file.name | WHERE MATCH(`_file.name`, \"foo\")"
        );
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Match match = as(filter.condition(), Match.class);
        assertThat(match.field(), instanceOf(ExternalMetadataAttribute.class));
        assertThat(filter.child(), instanceOf(ExternalRelation.class));

        // a _file.* column not named in METADATA is not in scope, so MATCH reports it like any other unknown column
        datasetError(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " | WHERE MATCH(`_file.name`, \"foo\")",
            containsString("Unknown column [_file.name]")
        );
    }

    public void testMetadataBoundColumnsSurviveKeepStar() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _id, _file.path | KEEP *");
        List<String> names = plan.output().stream().map(Attribute::name).toList();
        assertThat(names, hasItem("_id"));
        assertThat(names, hasItem("_file.path"));
        assertThat(names, hasItem("emp_no"));
        Attribute id = plan.output().stream().filter(a -> a.name().equals("_id")).findFirst().orElseThrow();
        Attribute path = plan.output().stream().filter(a -> a.name().equals("_file.path")).findFirst().orElseThrow();
        assertThat(id, instanceOf(ExternalMetadataAttribute.class));
        assertThat(path, instanceOf(ExternalMetadataAttribute.class));
    }

    public void testKeepStarDoesNotInventUnboundMetadata() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _file.size | KEEP *");
        List<String> names = plan.output().stream().map(Attribute::name).toList();
        assertThat(names, hasItem("_file.size"));
        assertThat(names, hasItem("emp_no"));
        assertThat(names, not(hasItem("_file.name")));
        assertThat(names, not(hasItem("_file.path")));
        Attribute size = plan.output().stream().filter(a -> a.name().equals("_file.size")).findFirst().orElseThrow();
        assertThat(size, instanceOf(ExternalMetadataAttribute.class));
    }

    public void testKeepStarWithoutMetadataOmitsFileColumns() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " | KEEP *");
        List<String> names = plan.output().stream().map(Attribute::name).toList();
        for (String name : FileMetadataColumns.NAMES) {
            assertThat(names, not(hasItem(name)));
        }
        assertThat(names, hasItem("emp_no"));
    }

    public void testDatasetOutputOmitsSyntheticAttributes() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _file.record_ref | KEEP *");
        List<String> names = plan.output().stream().map(Attribute::name).toList();
        assertThat(names, hasItem(FileMetadataColumns.RECORD_REF));
        assertThat(names, not(hasItem(ColumnExtractor.ROW_POSITION_COLUMN)));
        assertThat(plan.output().stream().filter(Attribute::synthetic).toList(), hasSize(0));
    }

    public void testKeepFilePatternMatchesOnlyBoundMetadata() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _file.size | KEEP _file*");
        List<String> names = plan.output().stream().map(Attribute::name).toList();
        assertEquals(List.of("_file.size"), names);
        assertThat(plan.output().getFirst(), instanceOf(ExternalMetadataAttribute.class));
    }

    public void testMetadataIdSurfacesWithoutKeep() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _id");
        List<String> names = plan.output().stream().map(Attribute::name).toList();
        assertThat(names, hasItem("_id"));
        Attribute id = plan.output().stream().filter(a -> a.name().equals("_id")).findFirst().orElseThrow();
        assertThat(id, instanceOf(ExternalMetadataAttribute.class));
    }

    public void testKeepStarWithExplicitNameKeepsMetadata() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        for (String keep : List.of("KEEP *, emp_no", "KEEP emp_no, *")) {
            var plan = analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _id | " + keep);
            List<String> names = plan.output().stream().map(Attribute::name).toList();
            assertThat(keep + " columns: " + names, names, hasItem("_id"));
            assertThat(keep + " columns: " + names, names, hasItem("emp_no"));
            Attribute id = plan.output().stream().filter(a -> a.name().equals("_id")).findFirst().orElseThrow();
            assertThat(keep + " _id type", id, instanceOf(ExternalMetadataAttribute.class));
        }
    }

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
        // column that must be named explicitly (it forces the reader's row-position channel), so
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
     * the registered type when listed in {@code METADATA} on an external dataset. {@code _id},
     * {@code _version} and {@code _source} bind like the rest and answer SQL NULL at execution
     * (pinned in {@code AbstractExternalMetadataMatrixIT#testAllStandardMetadataColumnsPinned}).
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

    public void testMetadataIdWinsOverPhysicalColumn() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var leafOutput = externalLeafOutput(analyzeDataset(externalCollision(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _id"));

        List<Attribute> ids = leafOutput.stream().filter(a -> a.name().equals("_id")).toList();
        assertThat(ids, hasSize(1));
        assertThat(ids.get(0), instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, ids.get(0).dataType());
        assertWarnings(Analyzer.shadowedExternalColumnsWarning(DATASET_NAME, List.of("_id")));
    }

    public void testMetadataFilePathWinsOverPhysicalColumn() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var leafOutput = externalLeafOutput(analyzeDataset(externalCollision(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _file.path"));

        List<Attribute> paths = leafOutput.stream().filter(a -> a.name().equals(FileMetadataColumns.PATH)).toList();
        assertThat(paths, hasSize(1));
        assertThat(paths.get(0), instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, paths.get(0).dataType());
        assertWarnings(Analyzer.shadowedExternalColumnsWarning(DATASET_NAME, List.of(FileMetadataColumns.PATH)));
    }

    public void testMetadataIdAndFilePathWinTogether() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var leafOutput = externalLeafOutput(
            analyzeDataset(externalCollision(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _id, _file.path")
        );

        Attribute id = leafOutput.stream().filter(a -> a.name().equals("_id")).findFirst().orElseThrow();
        Attribute path = leafOutput.stream().filter(a -> a.name().equals(FileMetadataColumns.PATH)).findFirst().orElseThrow();
        assertThat(id, instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, id.dataType());
        assertThat(path, instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, path.dataType());
        assertWarnings(Analyzer.shadowedExternalColumnsWarning(DATASET_NAME, List.of("_id", FileMetadataColumns.PATH)));
    }

    public void testMetadataIndexBindsWithoutWarningWhenNoCollision() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        var leafOutput = externalLeafOutput(analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _index"));

        Attribute index = leafOutput.stream().filter(a -> a.name().equals("_index")).findFirst().orElseThrow();
        assertThat(index, instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, index.dataType());
    }

    public void testDuplicateMetadataClauseRejectedByParser() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        Exception e = expectThrows(
            Exception.class,
            () -> analyzeDataset(external(), S3_PATH, "FROM " + DATASET_NAME + " METADATA _index, _index")
        );
        assertThat(e.getMessage(), containsString("already declared"));
    }

    public void testDuplicatePhysicalColumnsDroppedOnBind() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        List<Attribute> schema = List.of(
            referenceAttribute("emp_no", LONG),
            referenceAttribute("_id", LONG),
            referenceAttribute("_id", KEYWORD),
            referenceAttribute("first_name", KEYWORD)
        );
        var leafOutput = externalLeafOutput(
            analyzeDataset(
                analyzer().externalSourceUnresolved(datasetProject(S3_PATH), S3_PATH, schema),
                S3_PATH,
                "FROM " + DATASET_NAME + " METADATA _id"
            )
        );

        List<Attribute> ids = leafOutput.stream().filter(a -> a.name().equals("_id")).toList();
        assertThat(ids, hasSize(1));
        assertThat(ids.get(0), instanceOf(ExternalMetadataAttribute.class));
        assertEquals(KEYWORD, ids.get(0).dataType());
        assertWarnings(Analyzer.shadowedExternalColumnsWarning(DATASET_NAME, List.of("_id")));
    }

    public void testMetadataOrdinaryColumnIsUnresolvedPattern() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        datasetError(
            external(),
            S3_PATH,
            "FROM " + DATASET_NAME + " METADATA emp_no",
            containsString("Unresolved metadata pattern [emp_no]")
        );
    }

    public void testShadowWarningOmitsMappingAdviceWhenDatasetIsNull() {
        String warning = Analyzer.shadowedExternalColumnsWarning(null, List.of(FileMetadataColumns.SIZE));
        assertEquals(
            "Column [" + FileMetadataColumns.SIZE + "] in this source is shadowed by METADATA; the METADATA value is used",
            warning
        );
    }

    public void testShadowWarningIncludesMappingAdviceForDataset() {
        String warning = Analyzer.shadowedExternalColumnsWarning(DATASET_NAME, List.of("_id"));
        assertEquals(
            "Column [_id] in dataset ["
                + DATASET_NAME
                + "] is shadowed by METADATA; the METADATA value is used; rename it in the dataset mapping to read both",
            warning
        );
    }

    public void testShadowWarningPluralisesSeveralColumns() {
        String warning = Analyzer.shadowedExternalColumnsWarning(DATASET_NAME, List.of("_id", FileMetadataColumns.PATH));
        assertEquals(
            "Columns [_id], ["
                + FileMetadataColumns.PATH
                + "] in dataset ["
                + DATASET_NAME
                + "] are shadowed by METADATA; the METADATA value is used; rename them in the dataset mapping to read both",
            warning
        );
    }

    /**
     * Two relations on one resource, read under two configurations, each bind to <em>their own</em> resolution.
     * <p>
     * This is what the resolution key is for. The resolution holds both reads, under schemas that differ, and the
     * only thing that can tell {@code ResolveExternalRelations} which belongs to which relation is the relation's
     * configuration — so a lookup by resource alone binds both relations to whichever read it reaches first, and one
     * of them then reports a schema nobody asked for. The query that produces this shape in production is
     * {@code FROM sales_csv, sales_tsv} over two datasets sharing one resource.
     */
    public void testEachRelationOnOneResourceBindsToItsOwnResolution() {
        assumeTrue("requires dataset-in-FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        Map<String, Object> asCsv = Map.of("format", "csv", ExternalSourceResolver.DATASET_CONTEXT_KEY, Map.of("dataset", "sales_csv"));
        Map<String, Object> asTsv = Map.of("format", "tsv", ExternalSourceResolver.DATASET_CONTEXT_KEY, Map.of("dataset", "sales_tsv"));
        // One column apiece, named for the read, so each relation's own schema is visible in its output.
        List<Attribute> csvSchema = List.of(referenceAttribute("from_csv", INTEGER));
        List<Attribute> tsvSchema = List.of(referenceAttribute("from_tsv", KEYWORD));
        ExternalSourceResolution resolution = new ExternalSourceResolution(
            Map.of(
                new ExternalSourceSpec(S3_PATH, asCsv),
                resolvedSourceWith(csvSchema),
                new ExternalSourceSpec(S3_PATH, asTsv),
                resolvedSourceWith(tsvSchema)
            )
        );

        // A parsed FROM supplies the surrounding plan (the implicit LIMIT the verifier wants); its single relation is
        // replaced by the union of the two external reads DatasetRewriter would have produced.
        LogicalPlan parsed = TEST_PARSER.parseQuery("FROM sales");
        LogicalPlan plan = parsed.transformUp(
            UnresolvedRelation.class,
            r -> new UnionAll(
                r.source(),
                List.of(externalRelation(r.source(), asCsv, "sales_csv"), externalRelation(r.source(), asTsv, "sales_tsv")),
                List.of()
            )
        );

        LogicalPlan analyzed = analyzer().externalSourceResolution(resolution).buildAnalyzer().analyze(plan);

        Map<String, List<String>> columnsByDataset = new HashMap<>();
        analyzed.forEachDown(
            ExternalRelation.class,
            e -> columnsByDataset.put(e.datasetName(), e.output().stream().map(Attribute::name).toList())
        );
        assertThat("both relations must resolve", columnsByDataset.keySet(), equalTo(Set.of("sales_csv", "sales_tsv")));
        assertThat("the csv read must carry the csv schema", columnsByDataset.get("sales_csv"), equalTo(List.of("from_csv")));
        assertThat("the tsv read must carry the tsv schema", columnsByDataset.get("sales_tsv"), equalTo(List.of("from_tsv")));
    }

    /**
     * A dataset's list of structures is not one of its columns, as an index's {@code nested} field is not. Where the build
     * reads no nested columns no reader describes one (see {@code DataSourceModule}), and the list is an unsupported
     * column, as on main.
     */
    public void testNestedColumnIsNotADatasetColumn() {
        var output = externalLeafOutput(analyzeDataset(nestedDataset(null), S3_PATH, "FROM " + DATASET_NAME));
        assertThat(output.stream().map(Attribute::name).toList(), equalTo(List.of("question")));
    }

    /**
     * A dataset whose only column is a list of structures still has a column, as FROM over an index whose every field is
     * hidden does.
     */
    public void testAllNestedDatasetKeepsAColumn() {
        assumeNestedDatasets();
        var output = externalLeafOutput(
            analyzeDataset(
                nestedDataset((String) null, List.of(referenceAttribute("answers", DataType.UNSUPPORTED))),
                S3_PATH,
                "FROM " + DATASET_NAME
            )
        );
        assertThat(output, equalTo(Analyzer.NO_FIELDS));
    }

    /**
     * Only the unsupported column a list of structures reads as, or one its files disagree about, is hidden: a column of the
     * same name and another type, such as a partition column the path shadows, stays a column.
     */
    public void testColumnNamedLikeANestedColumnStaysAColumn() {
        assumeNestedDatasets();
        var output = externalLeafOutput(
            analyzeDataset(
                nestedDataset((String) null, List.of(referenceAttribute("question", KEYWORD), referenceAttribute("answers", KEYWORD))),
                S3_PATH,
                "FROM " + DATASET_NAME
            )
        );
        assertThat(output.stream().map(Attribute::name).toList(), equalTo(List.of("question", "answers")));
    }

    /** A list of structures the reader cannot read is hidden as any nested column is, as an index's nested field is. */
    public void testUnreadableNestedColumnIsHidden() {
        assumeNestedDatasets();
        TestAnalyzer dataset = nestedDataset(
            new NestedColumns.NestedColumn("answers", Map.of("type", KEYWORD), null, "[answers] holds more than [2] fields"),
            List.of(referenceAttribute("question", KEYWORD), referenceAttribute("answers", DataType.UNSUPPORTED))
        );
        var output = externalLeafOutput(analyzeDataset(dataset, S3_PATH, "FROM " + DATASET_NAME));
        assertThat(output.stream().map(Attribute::name).toList(), equalTo(List.of("question")));
    }

    /** NESTED_EXPAND binds the element fields schema discovery recorded, at their types. */
    public void testNestedExpandOverDatasetBindsElementColumns() {
        assumeNestedDatasets();
        var plan = analyzeDataset(
            nestedDataset(null),
            S3_PATH,
            "FROM " + DATASET_NAME + " | NESTED_EXPAND answers | KEEP question, answers.*"
        );
        assertThat(
            plan.output().stream().map(a -> a.name() + ":" + a.dataType().typeName()).toList(),
            equalTo(List.of("question:keyword", "answers.ttl:long", "answers.type:keyword"))
        );
    }

    /**
     * A list of structures whose files disagree is hidden, as an index's nested field is; NESTED_EXPAND of it says why it
     * cannot read it, at the command, through the scope's conflict.
     */
    public void testConflictedNestedColumnIsHiddenAndRefusedAtTheExpansion() {
        assumeNestedDatasets();
        String conflict = "[answers] is a list of structures in [f1.parquet] but not in [f2.parquet]";
        var output = externalLeafOutput(analyzeDataset(nestedDataset(conflict), S3_PATH, "FROM " + DATASET_NAME));
        assertThat(output.stream().map(Attribute::name).toList(), equalTo(List.of("question")));
        datasetError(
            nestedDataset(conflict),
            S3_PATH,
            "FROM " + DATASET_NAME + " | NESTED_EXPAND answers",
            containsString("1:33: NESTED_EXPAND cannot read [answers]: " + conflict)
        );
    }

    /**
     * A list of structures in one file and a plain column in another merge, under {@code union_by_name}, to a column that is not
     * unsupported; the column is still the nested one the files disagree about, so it is hidden and NESTED_EXPAND of it says
     * why.
     */
    public void testConflictedNestedColumnOfAnotherTypeIsHiddenAndRefusedAtTheExpansion() {
        assumeNestedDatasets();
        String conflict = "[answers] is a list of structures in [f1.parquet] but not in [f2.parquet]";
        List<Attribute> schema = List.of(referenceAttribute("question", KEYWORD), referenceAttribute("answers", KEYWORD));
        var output = externalLeafOutput(analyzeDataset(nestedDataset(conflict, schema), S3_PATH, "FROM " + DATASET_NAME));
        assertThat(output.stream().map(Attribute::name).toList(), equalTo(List.of("question")));
        datasetError(
            nestedDataset(conflict, schema),
            S3_PATH,
            "FROM " + DATASET_NAME + " | NESTED_EXPAND answers",
            containsString("1:33: NESTED_EXPAND cannot read [answers]: " + conflict)
        );
    }

    /**
     * An element field of a type the reader cannot read, such as a map inside the element, binds as an index's unsupported
     * nested field does ({@code ne_unsupported} in {@link AnalyzerNestedExpandTests}): an {@link UnsupportedAttribute}, kept
     * by name and by the expansion's every column.
     */
    public void testUnsupportedElementFieldBindsAsAnIndexUnsupportedNestedField() {
        assumeNestedDatasets();
        TestAnalyzer dataset = nestedDatasetWith(
            List.of(new NestedColumns.NestedColumn("items", Map.of("qty", LONG, "span", DataType.UNSUPPORTED), null)),
            List.of(referenceAttribute("id", KEYWORD), referenceAttribute("items", DataType.UNSUPPORTED))
        );
        TestAnalyzer index = AnalyzerNestedExpandTests.nestedAnalyzer(TransportVersion.current());
        for (String expansion : List.of(" | NESTED_EXPAND items | KEEP items.span", " | NESTED_EXPAND items")) {
            List<Attribute> fromDataset = analyzeDataset(dataset, S3_PATH, "FROM " + DATASET_NAME + expansion).output();
            List<Attribute> fromIndex = index.query("FROM ne_unsupported" + expansion).output();
            assertThat(expansion, describe(fromDataset), equalTo(describe(fromIndex)));
            Attribute span = fromDataset.stream().filter(a -> a.name().equals("items.span")).findFirst().orElseThrow();
            assertThat(expansion, span, instanceOf(UnsupportedAttribute.class));
        }
    }

    /**
     * Each expanded column's name, class and type. The parent's columns are left out: a dataset's are reference attributes, an
     * index's field attributes, whatever the expansion does.
     */
    private static List<String> describe(List<Attribute> attributes) {
        return attributes.stream()
            .filter(a -> a.name().startsWith("items."))
            .map(a -> a.name() + ":" + a.getClass().getSimpleName() + ":" + a.dataType().typeName())
            .toList();
    }

    /**
     * A partition key that shares the name of a list of structures the files disagree about shadows it, as it shadows any
     * column: the partition column is a column of its own, whatever the nested column's conflict says.
     */
    public void testPartitionColumnNamedLikeAConflictedNestedColumnStaysAColumn() {
        assumeNestedDatasets();
        String conflict = "[answers] is a list of structures in [f1.parquet] but not in [f2.parquet]";
        TestAnalyzer dataset = nestedDatasetWith(
            List.of(new NestedColumns.NestedColumn("answers", Map.of("type", KEYWORD, "ttl", LONG), conflict)),
            List.of(referenceAttribute("question", KEYWORD), referenceAttribute("answers", INTEGER)),
            Map.of(SourceStatisticsSerializer.PARTITION_COLUMNS_KEY, List.of("answers"))
        );
        var output = externalLeafOutput(analyzeDataset(dataset, S3_PATH, "FROM " + DATASET_NAME));
        assertThat(output.stream().map(Attribute::name).toList(), equalTo(List.of("question", "answers")));
    }

    /**
     * NESTED_EXPAND over a list of structures the reader cannot read is refused with the reader's reason at the command, and
     * only where the expansion binds it: the same query expanding another collection of the dataset runs.
     */
    public void testNestedExpandOverAnUnreadableColumnIsRefused() {
        assumeNestedDatasets();
        String unreadable = "[answers] holds more than [2] fields in each element, which cannot be read";
        TestAnalyzer dataset = nestedDatasetWith(
            List.of(
                new NestedColumns.NestedColumn("answers", Map.of("type", KEYWORD), null, unreadable),
                new NestedColumns.NestedColumn("hops", Map.of("ip", KEYWORD), null)
            ),
            List.of(
                referenceAttribute("question", KEYWORD),
                referenceAttribute("answers", DataType.UNSUPPORTED),
                referenceAttribute("hops", DataType.UNSUPPORTED)
            )
        );
        datasetError(
            dataset,
            S3_PATH,
            "FROM " + DATASET_NAME + " | NESTED_EXPAND answers",
            containsString("NESTED_EXPAND cannot read [answers]: " + unreadable)
        );
        var plan = analyzeDataset(dataset, S3_PATH, "FROM " + DATASET_NAME + " | NESTED_EXPAND hops | KEEP question, hops.ip");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("question", "hops.ip")));
    }

    /**
     * A collection inside another's element, as an index's nested field inside another: NESTED_EXPAND of the outer one
     * binds its own fields without the inner one's, as the index's {@code testBindsChildColumns} does.
     */
    public void testNestedExpandOmitsAnInnerCollection() {
        assumeNestedDatasets();
        var plan = analyzeDataset(innerCollectionDataset(), S3_PATH, "FROM " + DATASET_NAME + " | NESTED_EXPAND items | KEEP id, items.*");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("id", "items.qty", "items.sku")));
    }

    /** NESTED_EXPAND of an inner collection directly is refused as on an index ({@code testOneNestingLevel}). */
    public void testNestedExpandOfAnInnerCollectionIsOneLevelTooDeep() {
        assumeNestedDatasets();
        datasetError(
            innerCollectionDataset(),
            S3_PATH,
            "FROM " + DATASET_NAME + " | NESTED_EXPAND items.discounts",
            containsString("NESTED_EXPAND supports one nesting level")
        );
    }

    /**
     * A second NESTED_EXPAND of the inner collection after the outer one finds no such column, as on an index: the outer
     * expansion removed every scope under its path ({@code NestedExpand.expandedScope}).
     */
    public void testSecondExpansionOfAnInnerCollectionIsUnknown() {
        assumeNestedDatasets();
        datasetError(
            innerCollectionDataset(),
            S3_PATH,
            "FROM " + DATASET_NAME + " | NESTED_EXPAND items | NESTED_EXPAND items.discounts",
            containsString("Unknown column [items.discounts]")
        );
    }

    /** {@code items} holding {@code qty} and {@code sku}, and inside its element the collection {@code items.discounts}. */
    private static TestAnalyzer innerCollectionDataset() {
        return nestedDatasetWith(
            List.of(
                new NestedColumns.NestedColumn("items", Map.of("qty", LONG, "sku", KEYWORD), null),
                new NestedColumns.NestedColumn("items.discounts", Map.of("code", KEYWORD, "pct", LONG), null)
            ),
            List.of(referenceAttribute("id", LONG), referenceAttribute("items", DataType.UNSUPPORTED))
        );
    }

    /** NESTED_ANY over a dataset is not supported, also in a plan whose NESTED_EXPAND gives the dataset its nested scope. */
    public void testNestedAnyOverDatasetRefused() {
        assumeNestedDatasets();
        datasetError(
            nestedDataset(null),
            S3_PATH,
            "FROM " + DATASET_NAME + " | WHERE NESTED_ANY(answers, answers.ttl < 60) | NESTED_EXPAND answers",
            containsString("NESTED_ANY cannot read [answers] from a dataset")
        );
        datasetError(
            nestedDataset(null),
            S3_PATH,
            "FROM " + DATASET_NAME + " | WHERE NESTED_ANY(answers, answers.ttl < 60)",
            containsString("Unknown column [answers]")
        );
    }

    /**
     * Every node must be able to run NESTED_EXPAND over a dataset. The refusal is the collection's scope conflict, so it is
     * raised at the command that binds the dataset's collection, on a cluster whose nodes all run NESTED_EXPAND on an index.
     */
    public void testNestedExpandOverDatasetRequiresAllNodes() {
        assumeNestedDatasets();
        TransportVersion beforeDatasets = TransportVersionUtils.getPreviousVersion(NestedExpand.DATASET_VERSION);
        assertTrue(beforeDatasets.supports(NestedExpand.VERSION));
        datasetError(
            nestedDataset(null).minimumTransportVersion(beforeDatasets),
            S3_PATH,
            "FROM " + DATASET_NAME + " | NESTED_EXPAND answers",
            containsString("1:33: NESTED_EXPAND cannot read [answers]: NESTED_EXPAND over a dataset requires all nodes to support it")
        );
        // A plan that only reads the dataset runs on those nodes
        analyzeDataset(nestedDataset(null).minimumTransportVersion(beforeDatasets), S3_PATH, "FROM " + DATASET_NAME);
    }

    private static void assumeNestedDatasets() {
        assumeTrue("NESTED_EXPAND over datasets is snapshot-only", EsqlCapabilities.Cap.NESTED_EXPAND_DATASET.isEnabled());
    }

    /**
     * A dataset with {@code question} and a list of structures {@code answers} whose elements hold {@code type} and
     * {@code ttl}, as schema discovery describes it; {@code conflict} is the reason the list cannot be read, or {@code null}.
     */
    private static TestAnalyzer nestedDataset(String conflict) {
        return nestedDataset(
            conflict,
            List.of(referenceAttribute("question", KEYWORD), referenceAttribute("answers", DataType.UNSUPPORTED))
        );
    }

    /** As {@link #nestedDataset(String)}, with the file columns {@code schema}. */
    private static TestAnalyzer nestedDataset(String conflict, List<Attribute> schema) {
        return nestedDataset(new NestedColumns.NestedColumn("answers", Map.of("type", KEYWORD, "ttl", LONG), conflict), schema);
    }

    /** A dataset with the file columns {@code schema} and the one nested column {@code column}. */
    private static TestAnalyzer nestedDataset(NestedColumns.NestedColumn column, List<Attribute> schema) {
        return nestedDatasetWith(List.of(column), schema);
    }

    /** A dataset with the file columns {@code schema} and the nested columns {@code columns}. */
    private static TestAnalyzer nestedDatasetWith(List<NestedColumns.NestedColumn> columns, List<Attribute> schema) {
        return nestedDatasetWith(columns, schema, Map.of());
    }

    /** As {@link #nestedDatasetWith(List, List)}, with the further source metadata {@code extra}. */
    private static TestAnalyzer nestedDatasetWith(
        List<NestedColumns.NestedColumn> columns,
        List<Attribute> schema,
        Map<String, Object> extra
    ) {
        Map<String, Object> metadata = NestedColumns.of(columns).writeTo(extra);
        var source = new ExternalSourceResolution.ResolvedSource(new ExternalSourceMetadata() {
            @Override
            public String location() {
                return S3_PATH;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return metadata;
            }
        }, FileList.UNRESOLVED, Map.of());
        return analyzer().minimumTransportVersion(TransportVersion.current())
            .externalSourceResolution(externalResolutionFor(datasetProject(S3_PATH), Map.of(S3_PATH, source)));
    }

    /** An {@link UnresolvedExternalRelation} on {@link #S3_PATH} under {@code config}, as DatasetRewriter builds one. */
    private static UnresolvedExternalRelation externalRelation(Source source, Map<String, Object> config, String datasetName) {
        return new UnresolvedExternalRelation(source, Literal.keyword(source, S3_PATH), config, List.of(), datasetName);
    }

    /** A resolved source over {@code schema}, with nothing listed: this case is about which schema binds where. */
    private static ExternalSourceResolution.ResolvedSource resolvedSourceWith(List<Attribute> schema) {
        return new ExternalSourceResolution.ResolvedSource(new ExternalSourceMetadata() {
            @Override
            public String location() {
                return S3_PATH;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        }, FileList.UNRESOLVED, Map.of());
    }

    /**
     * Returns the {@link ExternalRelation} leaf's output from an analyzed plan. The leaf's output
     * carries every name bound by {@code ResolveExternalRelations}, which is the binding contract
     * under test.
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
        LogicalPlan rewritten = rewriteDatasetsUnsecured(
            TEST_PARSER.parseQuery(query),
            datasetProject(resource),
            TestIndexNameExpressionResolver.newInstance(),
            // These cases name their datasets exactly, which reaches them at the wildcards_match_datasets default.
            false
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
        Dataset dataset = new Dataset(DATASET_NAME, new DataSourceReference("ds_source"), resource, null, Map.of(), null);
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("ds_source", dataSource)))
            .datasets(Map.of(DATASET_NAME, dataset))
            .build();
    }

    /**
     * The analyzer for this class's {@code FROM <dataset>} cases: the resolution is keyed for the relations
     * {@link #datasetProject} rewrites to, which carry the dataset's config. For a relation with no dataset behind it,
     * whose relation carries no config, use {@link #externalCommand()}.
     */
    public static TestAnalyzer external() {
        return analyzer().externalSourceUnresolved(datasetProject(S3_PATH), S3_PATH, employeesSchema());
    }

    /** As {@link #external()}, keyed for the empty config a relation with no dataset behind it carries. */
    public static TestAnalyzer externalCommand() {
        return analyzer().externalSourceUnresolved(S3_PATH, employeesSchema());
    }

    private static TestAnalyzer externalCollision() {
        return analyzer().externalSourceUnresolved(datasetProject(S3_PATH), S3_PATH, collisionSchema());
    }

    private static List<Attribute> collisionSchema() {
        return List.of(
            referenceAttribute("emp_no", LONG),
            referenceAttribute("_id", LONG),
            referenceAttribute(FileMetadataColumns.PATH, KEYWORD),
            referenceAttribute("first_name", KEYWORD)
        );
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
