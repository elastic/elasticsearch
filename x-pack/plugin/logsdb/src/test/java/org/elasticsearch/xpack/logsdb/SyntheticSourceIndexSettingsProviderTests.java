/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SyntheticSourceIndexSettingsProviderTests extends ESTestCase {

    private SyntheticSourceLicenseService syntheticSourceLicenseService;
    private SyntheticSourceIndexSettingsProvider provider;

    private static LogsdbIndexModeSettingsProvider getLogsdbIndexModeSettingsProvider(boolean enabled) {
        return new LogsdbIndexModeSettingsProvider(Settings.builder().put("cluster.logsdb.enabled", enabled).build());
    }

    @Before
    public void setup() {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(any())).thenReturn(true);
        var licenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        licenseService.setLicenseState(licenseState);
        syntheticSourceLicenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        syntheticSourceLicenseService.setLicenseState(licenseState);

        provider = new SyntheticSourceIndexSettingsProvider(
            syntheticSourceLicenseService,
            im -> MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), im.getSettings(), im.getIndex().getName()),
            getLogsdbIndexModeSettingsProvider(false)
        );
    }

    public void testNewIndexHasSyntheticSourceUsage() throws IOException {
        String dataStreamName = "logs-app1";
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 0);
        Settings settings = Settings.EMPTY;
        {
            String mapping = """
                {
                    "_doc": {
                        "_source": {
                            "mode": "synthetic"
                        },
                        "properties": {
                            "my_field": {
                                "type": "keyword"
                            }
                        }
                    }
                }
                """;
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, settings, List.of(new CompressedXContent(mapping)));
            assertTrue(result);
        }
        {
            String mapping;
            if (randomBoolean()) {
                mapping = """
                    {
                        "_doc": {
                            "_source": {
                                "mode": "stored"
                            },
                            "properties": {
                                "my_field": {
                                    "type": "keyword"
                                }
                            }
                        }
                    }
                    """;
            } else {
                mapping = """
                    {
                        "_doc": {
                            "properties": {
                                "my_field": {
                                    "type": "keyword"
                                }
                            }
                        }
                    }
                    """;
            }
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, settings, List.of(new CompressedXContent(mapping)));
            assertFalse(result);
        }
    }

    public void testValidateIndexName() throws IOException {
        String indexName = "validate-index-name";
        String mapping = """
            {
                "_doc": {
                    "_source": {
                        "mode": "synthetic"
                    },
                    "properties": {
                        "my_field": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
        Settings settings = Settings.EMPTY;
        boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, settings, List.of(new CompressedXContent(mapping)));
        assertFalse(result);
    }

    public void testNewIndexHasSyntheticSourceUsageLogsdbIndex() throws IOException {
        String dataStreamName = "logs-app1";
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 0);
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "my_field": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
        {
            Settings settings = Settings.builder().put("index.mode", "logsdb").build();
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, settings, List.of(new CompressedXContent(mapping)));
            assertTrue(result);
        }
        {
            Settings settings = Settings.builder().put("index.mode", "logsdb").build();
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, settings, List.of());
            assertTrue(result);
        }
        {
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, Settings.EMPTY, List.of());
            assertFalse(result);
        }
        {
            boolean result = provider.newIndexHasSyntheticSourceUsage(
                indexName,
                null,
                Settings.EMPTY,
                List.of(new CompressedXContent(mapping))
            );
            assertFalse(result);
        }
    }

    public void testNewIndexHasSyntheticSourceUsageTimeSeries() throws IOException {
        String dataStreamName = "logs-app1";
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 0);
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "my_field": {
                            "type": "keyword",
                            "time_series_dimension": true
                        }
                    }
                }
            }
            """;
        {
            Settings settings = Settings.builder().put("index.mode", "time_series").put("index.routing_path", "my_field").build();
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, settings, List.of(new CompressedXContent(mapping)));
            assertTrue(result);
        }
        {
            Settings settings = Settings.builder().put("index.mode", "time_series").put("index.routing_path", "my_field").build();
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, settings, List.of());
            assertTrue(result);
        }
        {
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, Settings.EMPTY, List.of());
            assertFalse(result);
        }
        {
            boolean result = provider.newIndexHasSyntheticSourceUsage(
                indexName,
                null,
                Settings.EMPTY,
                List.of(new CompressedXContent(mapping))
            );
            assertFalse(result);
        }
    }

    public void testNewIndexHasSyntheticSourceUsage_invalidSettings() throws IOException {
        String dataStreamName = "logs-app1";
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 0);
        Settings settings = Settings.builder().put("index.soft_deletes.enabled", false).build();
        {
            String mapping = """
                {
                    "_doc": {
                        "_source": {
                            "mode": "synthetic"
                        },
                        "properties": {
                            "my_field": {
                                "type": "keyword"
                            }
                        }
                    }
                }
                """;
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, settings, List.of(new CompressedXContent(mapping)));
            assertFalse(result);
        }
        {
            String mapping = """
                {
                    "_doc": {
                        "properties": {
                            "my_field": {
                                "type": "keyword"
                            }
                        }
                    }
                }
                """;
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, settings, List.of(new CompressedXContent(mapping)));
            assertFalse(result);
        }
    }

    public void testGetAdditionalIndexSettingsDowngradeFromSyntheticSource() throws IOException {
        String dataStreamName = "logs-app1";
        Metadata.Builder mb = Metadata.builder(
            DataStreamTestHelper.getClusterStateWithDataStreams(
                List.of(Tuple.tuple(dataStreamName, 1)),
                List.of(),
                Instant.now().toEpochMilli(),
                builder().build(),
                1
            ).getMetadata()
        );
        Metadata metadata = mb.build();

        Settings settings = builder().put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC)
            .build();

        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            dataStreamName,
            null,
            metadata,
            Instant.ofEpochMilli(1L),
            settings,
            List.of()
        );
        assertThat(result.size(), equalTo(0));

        syntheticSourceLicenseService.setSyntheticSourceFallback(true);
        result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            dataStreamName,
            null,
            metadata,
            Instant.ofEpochMilli(1L),
            settings,
            List.of()
        );
        assertThat(result.size(), equalTo(1));
        assertEquals(SourceFieldMapper.Mode.STORED, SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.get(result));

        result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            dataStreamName,
            IndexMode.TIME_SERIES,
            metadata,
            Instant.ofEpochMilli(1L),
            settings,
            List.of()
        );
        assertThat(result.size(), equalTo(1));
        assertEquals(SourceFieldMapper.Mode.STORED, SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.get(result));

        result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            dataStreamName,
            IndexMode.LOGSDB,
            metadata,
            Instant.ofEpochMilli(1L),
            settings,
            List.of()
        );
        assertThat(result.size(), equalTo(1));
        assertEquals(SourceFieldMapper.Mode.STORED, SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.get(result));
    }

    public void testGetAdditionalIndexSettingsDowngradeFromSyntheticSourceFileMatch() throws IOException {
        syntheticSourceLicenseService.setSyntheticSourceFallback(true);
        provider = new SyntheticSourceIndexSettingsProvider(
            syntheticSourceLicenseService,
            im -> MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), im.getSettings(), im.getIndex().getName()),
            getLogsdbIndexModeSettingsProvider(true)
        );
        final Settings settings = Settings.EMPTY;

        String dataStreamName = "logs-app1";
        Metadata.Builder mb = Metadata.builder(
            DataStreamTestHelper.getClusterStateWithDataStreams(
                List.of(Tuple.tuple(dataStreamName, 1)),
                List.of(),
                Instant.now().toEpochMilli(),
                builder().build(),
                1
            ).getMetadata()
        );
        Metadata metadata = mb.build();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            dataStreamName,
            null,
            metadata,
            Instant.ofEpochMilli(1L),
            settings,
            List.of()
        );
        assertThat(result.size(), equalTo(0));

        dataStreamName = "logs-app1-0";
        mb = Metadata.builder(
            DataStreamTestHelper.getClusterStateWithDataStreams(
                List.of(Tuple.tuple(dataStreamName, 1)),
                List.of(),
                Instant.now().toEpochMilli(),
                builder().build(),
                1
            ).getMetadata()
        );
        metadata = mb.build();

        result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            dataStreamName,
            null,
            metadata,
            Instant.ofEpochMilli(1L),
            settings,
            List.of()
        );
        assertThat(result.size(), equalTo(1));
        assertEquals(SourceFieldMapper.Mode.STORED, SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.get(result));

        result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            dataStreamName,
            null,
            metadata,
            Instant.ofEpochMilli(1L),
            builder().put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.toString()).build(),
            List.of()
        );
        assertThat(result.size(), equalTo(0));
    }
}
