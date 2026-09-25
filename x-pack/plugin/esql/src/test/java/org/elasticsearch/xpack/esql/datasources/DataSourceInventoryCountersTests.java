/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.DatasetShape;

import java.util.Collection;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DataSourceInventoryCountersTests extends ESTestCase {

    public void testDoneWhenAnonymousS3CsvGzDeclaredStrict() {
        DataSourceValidator validator = s3AnonymousCsvGzip();
        ProjectMetadata project = doneWhenProject();

        Counters counters = new Counters();
        DataSourceInventoryCounters.populate(project, counters, type -> validator, null);
        assertThat(counters.get("datasources.config.datasources.by_type.s3"), equalTo(1L));
        assertThat(counters.get("datasources.config.datasources.by_auth.anonymous"), equalTo(1L));
        assertThat(counters.get("datasources.config.datasets.by_format.csv"), equalTo(1L));
        assertThat(counters.get("datasources.config.datasets.by_schema.declared_strict"), equalTo(1L));
        assertThat(counters.get("datasources.config.datasets.by_partitioning.auto"), equalTo(1L));
        assertThat(counters.get("datasources.config.datasets.by_compression.gzip"), equalTo(1L));

        Collection<LongWithAttributes> datasources = DataSourceInventoryCounters.datasourceObservations(project, type -> validator);
        assertThat(datasources, hasSize(1));
        LongWithAttributes datasource = datasources.iterator().next();
        assertThat(datasource.value(), equalTo(1L));
        assertThat(datasource.attributes().get(DataSourceInventoryCounters.TYPE_ATTRIBUTE), equalTo("s3"));
        assertThat(datasource.attributes().get(DataSourceInventoryCounters.AUTH_ATTRIBUTE), equalTo("anonymous"));

        Collection<LongWithAttributes> datasets = DataSourceInventoryCounters.datasetObservations(project, type -> validator, null);
        assertThat(datasets, hasSize(1));
        LongWithAttributes dataset = datasets.iterator().next();
        assertThat(dataset.value(), equalTo(1L));
        assertThat(dataset.attributes().get(DataSourceInventoryCounters.TYPE_ATTRIBUTE), equalTo("s3"));
        assertThat(dataset.attributes().get(DataSourceInventoryCounters.FORMAT_ATTRIBUTE), equalTo("csv"));
        assertThat(dataset.attributes().get(DataSourceInventoryCounters.SCHEMA_ATTRIBUTE), equalTo("declared_strict"));
        assertThat(dataset.attributes().get(DataSourceInventoryCounters.PARTITIONING_ATTRIBUTE), equalTo("auto"));
        assertThat(dataset.attributes().get(DataSourceInventoryCounters.COMPRESSION_ATTRIBUTE), equalTo("gzip"));
    }

    public void testStarResourceDoesNotChangeByType() {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(
                DataSourceMetadata.TYPE,
                new DataSourceMetadata(
                    Map.of(
                        "ds-s3",
                        new DataSource("ds-s3", "s3", null, Map.of()),
                        "ds-custom",
                        new DataSource("ds-custom", "custom_plugin", null, Map.of())
                    )
                )
            )
            .putCustom(
                DatasetMetadata.TYPE,
                new DatasetMetadata(
                    Map.of(
                        "view-s3",
                        new Dataset("view-s3", new DataSourceReference("ds-s3"), "*", null, Map.of()),
                        "view-ghost",
                        new Dataset("view-ghost", new DataSourceReference("ghost-ds"), "*", null, Map.of())
                    )
                )
            )
            .build();

        Counters counters = new Counters();
        DataSourceInventoryCounters.populate(project, counters, type -> null, null);
        assertThat(counters.get("datasources.config.datasources.count"), equalTo(2L));
        assertThat(counters.get("datasources.config.datasources.by_type.s3"), equalTo(1L));
        assertThat(counters.get("datasources.config.datasources.by_type.unknown"), equalTo(1L));
        assertThat(counters.get("datasources.config.datasources.by_type.gcs"), equalTo(0L));
        assertThat(counters.get("datasources.config.datasets.count"), equalTo(2L));
        assertThat(counters.get("datasources.config.datasets.by_datasource_type.s3"), equalTo(1L));
        assertThat(counters.get("datasources.config.datasets.by_datasource_type.unknown"), equalTo(1L));
        assertThat(counters.get("datasources.config.datasets.by_format.unresolved"), equalTo(2L));
    }

    public void testMappingMappingsNullIsInferred() {
        Dataset dataset = new Dataset(
            "emp",
            new DataSourceReference("ds-s3"),
            "s3://bucket/data.csv",
            null,
            Map.of(),
            new DatasetMapping((DatasetMapping.Mappings) null)
        );
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("ds-s3", new DataSource("ds-s3", "s3", null, Map.of()))))
            .putCustom(DatasetMetadata.TYPE, new DatasetMetadata(Map.of("emp", dataset)))
            .build();
        Counters counters = new Counters();
        DataSourceInventoryCounters.populate(project, counters, type -> null, null);
        assertThat(counters.get("datasources.config.datasets.by_schema.inferred"), equalTo(1L));
    }

    public void testDuplicateAnonymousS3GaugesAggregate() {
        DataSourceValidator validator = s3AnonymousCsvGzip();
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(
                DataSourceMetadata.TYPE,
                new DataSourceMetadata(
                    Map.of("ds-a", new DataSource("ds-a", "s3", null, Map.of()), "ds-b", new DataSource("ds-b", "s3", null, Map.of()))
                )
            )
            .putCustom(
                DatasetMetadata.TYPE,
                new DatasetMetadata(
                    Map.of(
                        "emp-a",
                        new Dataset(
                            "emp-a",
                            new DataSourceReference("ds-a"),
                            "s3://bucket/a.csv.gz",
                            null,
                            Map.of(),
                            new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, Map.of()))
                        ),
                        "emp-b",
                        new Dataset(
                            "emp-b",
                            new DataSourceReference("ds-b"),
                            "s3://bucket/b.csv.gz",
                            null,
                            Map.of(),
                            new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, Map.of()))
                        )
                    )
                )
            )
            .build();

        Counters counters = new Counters();
        DataSourceInventoryCounters.populate(project, counters, type -> validator, null);
        assertThat(counters.get("datasources.config.datasources.by_type.s3"), equalTo(2L));
        assertThat(counters.get("datasources.config.datasources.by_auth.anonymous"), equalTo(2L));
        assertThat(counters.get("datasources.config.datasets.by_format.csv"), equalTo(2L));

        Collection<LongWithAttributes> datasources = DataSourceInventoryCounters.datasourceObservations(project, type -> validator);
        assertThat(datasources, hasSize(1));
        LongWithAttributes datasource = datasources.iterator().next();
        assertThat(datasource.value(), equalTo(2L));
        assertThat(datasource.attributes().get(DataSourceInventoryCounters.TYPE_ATTRIBUTE), equalTo("s3"));
        assertThat(datasource.attributes().get(DataSourceInventoryCounters.AUTH_ATTRIBUTE), equalTo("anonymous"));

        Collection<LongWithAttributes> datasets = DataSourceInventoryCounters.datasetObservations(project, type -> validator, null);
        assertThat(datasets, hasSize(1));
        assertThat(datasets.iterator().next().value(), equalTo(2L));
    }

    public void testAuthFactoryThrowIsUnknownAndDoesNotDropType() {
        DataSourceValidator throwing = new DataSourceValidator() {
            @Override
            public String type() {
                return "s3";
            }

            @Override
            public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
                throw new ValidationException();
            }

            @Override
            public Map<String, Object> validateDataset(
                Map<String, DataSourceSetting> datasourceSettings,
                String resource,
                Map<String, Object> datasetSettings
            ) {
                return datasetSettings;
            }

            @Override
            public String authModeOrNull(Map<String, DataSourceSetting> stored) {
                throw new ValidationException();
            }
        };
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("ds-s3", new DataSource("ds-s3", "s3", null, Map.of()))))
            .build();
        Counters counters = new Counters();
        DataSourceInventoryCounters.populate(project, counters, type -> throwing, null);
        assertThat(counters.get("datasources.config.datasources.by_type.s3"), equalTo(1L));
        assertThat(counters.get("datasources.config.datasources.by_auth.unknown"), equalTo(1L));
    }

    private static ProjectMetadata doneWhenProject() {
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("ds-s3", new DataSource("ds-s3", "s3", null, Map.of()))))
            .putCustom(
                DatasetMetadata.TYPE,
                new DatasetMetadata(
                    Map.of(
                        "emp",
                        new Dataset(
                            "emp",
                            new DataSourceReference("ds-s3"),
                            "s3://bucket/data.csv.gz",
                            null,
                            Map.of(),
                            new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, Map.of()))
                        )
                    )
                )
            )
            .build();
    }

    private static DataSourceValidator s3AnonymousCsvGzip() {
        return new DataSourceValidator() {
            @Override
            public String type() {
                return "s3";
            }

            @Override
            public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
                throw new ValidationException();
            }

            @Override
            public Map<String, Object> validateDataset(
                Map<String, DataSourceSetting> datasourceSettings,
                String resource,
                Map<String, Object> datasetSettings
            ) {
                return datasetSettings;
            }

            @Override
            public String authModeOrNull(Map<String, DataSourceSetting> stored) {
                return "anonymous";
            }

            @Override
            public DatasetShape datasetShape(Map<String, Object> datasetSettings, String resource) {
                return new DatasetShape("csv", "gzip");
            }
        };
    }
}
