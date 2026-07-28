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
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneRedundantAggregateGroupings;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Golden (characterization) snapshot of the optimized logical plan for the {@link PruneRedundantAggregateGroupings}
 * external-grouping path. See GoldenTestsReadme.md.
 */
public class PruneRedundantAggregateGroupingsGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);
    private static final String DATASET_NAME = "ext_ds";
    private static final String DATA_RESOURCE = "s3://bucket/data.parquet";

    /**
     * Renaming an external column, deriving a value from it, then grouping by both: the derived grouping is pruned and
     * rebuilt above the aggregate reading the rename alias, so the plan stays consistent.
     */
    public void testRenamedDerivedExternalGrouping() {
        assumeTrue("requires FROM <dataset> capability", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM ext_ds
            | RENAME ClientIP AS cip
            | EVAL c = cip - 1
            | STATS count = COUNT(*) BY cip, c
            """;
        builder(query).stages(STAGES)
            .transportVersion(TransportVersion.current())
            .datasetMetadata(datasetMetadata())
            .externalSourceResolution(externalSourceResolution())
            .run();
    }

    /** Registers {@code ext_ds} as an external dataset so {@code FROM ext_ds} becomes an external relation. */
    private static ProjectMetadata datasetMetadata() {
        DataSource dataSource = new DataSource("ext_ds_ds", "test", null, Map.of());
        Dataset dataset = new Dataset(DATASET_NAME, new DataSourceReference("ext_ds_ds"), DATA_RESOURCE, null, Map.of());
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("ext_ds_ds", dataSource)))
            .datasets(Map.of(DATASET_NAME, dataset))
            .build();
    }

    private static ExternalSourceResolution externalSourceResolution() {
        List<Attribute> schema = List.of(
            referenceAttribute("ClientIP", DataType.INTEGER),
            referenceAttribute("OtherIP", DataType.INTEGER),
            referenceAttribute("URL", DataType.KEYWORD)
        );
        ExternalSourceMetadata metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return DATA_RESOURCE;
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
            Map.of(DATA_RESOURCE, new ExternalSourceResolution.ResolvedSource(metadata, FileList.UNRESOLVED, Map.of()))
        );
    }
}
