/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Golden tests for filter and sort pushdown to Lucene.
 * New pushdown tests should be added here rather than in {@link LocalPhysicalPlanOptimizerTests}.
 */
public class PushdownGoldenTests extends UnmappedGoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION);

    private static final String SALARIES_RESOURCE = "s3://bucket/golden_salaries.parquet";

    public void testFilterPushdownNoUnmapped() {
        String query = """
            FROM sample_data
            | KEEP message
            | WHERE message == "Connection error?"
            """;
        runGoldenTest(query, STAGES);
    }

    public void testFilterPushdownNoUnmappedFilterOnly() {
        String query = """
            FROM sample_data
            | KEEP message
            | WHERE message == "Connection error?"
            """;
        runGoldenTest(query, STAGES);
    }

    public void testFilterNoPushdownWithUnmapped() {
        String query = """
            FROM sample_data
            | KEEP message, does_not_exist
            | WHERE does_not_exist::KEYWORD == "Connection error?"
            """;
        runUnmappedTests(query);
    }

    public void testSortPushdownNoUnmapped() {
        String query = """
            FROM sample_data
            | KEEP message
            | SORT message
            | LIMIT 5
            """;
        runGoldenTest(query, STAGES);
    }

    public void testSortNoPushdownWithUnmapped() {
        String query = """
            FROM sample_data
            | KEEP message, does_not_exist
            | SORT does_not_exist
            | LIMIT 5
            """;
        runUnmappedTests(query);
    }

    public void testFilterConjunctionPushableAndNonPushable() {
        String query = """
            FROM sample_data
            | KEEP message, does_not_exist
            | WHERE message == "Connection error?" AND does_not_exist::KEYWORD == "foo"
            """;
        runUnmappedTests(query);
    }

    public void testFilterDisjunctionPushableAndNonPushable() {
        String query = """
            FROM sample_data
            | KEEP message, does_not_exist
            | WHERE message == "Connection error?" OR does_not_exist::KEYWORD == "foo"
            """;
        runUnmappedTests(query);
    }

    public void testSortConjunctionPushableAndNonPushable() {
        String query = """
            FROM sample_data
            | KEEP message, does_not_exist
            | SORT message, does_not_exist
            | LIMIT 5
            """;
        runUnmappedTests(query);
    }

    public void testStartsWithOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE starts_with(_index, "sample")
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testEndsWithOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE ends_with(_index, "data")
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testLikeOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE _index LIKE "sample*"
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testLikeSingleCharOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE _index LIKE "sample_dat?"
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testLikeListOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE _index LIKE ("sample*", "no_match*")
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testRlikeOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE _index RLIKE "sample_.*"
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testRlikeListOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE _index RLIKE ("sample_.*", "no_match.*")
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testStartsWithOnDashedIndex() {
        String query = """
            FROM k8s-downsampled METADATA _index
            | WHERE starts_with(_index, "k8s-")
            | KEEP cluster
            """;
        runGoldenTest(query, STAGES);
    }

    public void testEndsWithOnDashedIndex() {
        String query = """
            FROM k8s-downsampled METADATA _index
            | WHERE ends_with(_index, "-downsampled")
            | KEEP cluster
            """;
        runGoldenTest(query, STAGES);
    }

    public void testLikeOnDashedIndex() {
        String query = """
            FROM k8s-downsampled METADATA _index
            | WHERE _index LIKE "k8s-*"
            | KEEP cluster
            """;
        runGoldenTest(query, STAGES);
    }

    public void testFromUnionOfIndexTimeSeriesAndExternalDatasetSubqueries() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires external data source subquery support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM (FROM employees | EVAL name = first_name | KEEP name),
                 (FROM k8s | EVAL name = cluster | KEEP name),
                 (FROM golden_salaries | KEEP name)
            | WHERE name == "staging"
            | SORT name
            """;
        builder(query).stages(STAGES)
            .transportVersion(TransportVersion.current())
            .datasetMetadata(salariesDatasetMetadata())
            .externalSourceResolution(salariesExternalSourceResolution())
            .run();
    }

    public void testFromUnionOfIndexTimeSeriesRateAndExternalDatasetSubqueries() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires TS source inside a FROM subquery", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires external data source subquery support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM (FROM employees | EVAL name = first_name | KEEP name),
                 (TS k8s
                  | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
                  | WHERE max_rate > 0
                  | EVAL name = cluster
                  | KEEP name),
                 (FROM golden_salaries | KEEP name)
            | WHERE name == "staging"
            | SORT name
            """;
        builder(query).stages(STAGES)
            .transportVersion(TransportVersion.current())
            .datasetMetadata(salariesDatasetMetadata())
            .externalSourceResolution(salariesExternalSourceResolution())
            .run();
    }

    /** Registers {@code golden_salaries} as an external dataset so {@code FROM golden_salaries} becomes an external relation. */
    private static ProjectMetadata salariesDatasetMetadata() {
        DataSource dataSource = new DataSource("golden_ds", "test", null, Map.of());
        Dataset salaries = new Dataset("golden_salaries", new DataSourceReference("golden_ds"), SALARIES_RESOURCE, null, Map.of());
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("golden_ds", dataSource)))
            .datasets(Map.of("golden_salaries", salaries))
            .build();
    }

    /** Pre-resolved {@code emp_no}/{@code name}/{@code salary} schema for the {@code golden_salaries} external dataset. */
    private static ExternalSourceResolution salariesExternalSourceResolution() {
        List<Attribute> schema = List.of(
            referenceAttribute("emp_no", DataType.INTEGER),
            referenceAttribute("name", DataType.KEYWORD),
            referenceAttribute("salary", DataType.DOUBLE)
        );
        ExternalSourceMetadata metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return SALARIES_RESOURCE;
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
        return new ExternalSourceResolution(
            Map.of(SALARIES_RESOURCE, new ExternalSourceResolution.ResolvedSource(metadata, FileList.UNRESOLVED, Map.of()))
        );
    }

    private void runUnmappedTests(String query) {
        runTestsNullifyAndLoad(query, STAGES, null);
    }
}
