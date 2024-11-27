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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.xpack.logsdb.SyntheticSourceLicenseServiceTests.createEnterpriseLicense;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SyntheticSourceIndexSettingsProviderTests extends ESTestCase {

    private SyntheticSourceLicenseService syntheticSourceLicenseService;
    private SyntheticSourceIndexSettingsProvider provider;
    private final AtomicInteger newMapperServiceCounter = new AtomicInteger();

    static LogsdbIndexModeSettingsProvider getLogsdbIndexModeSettingsProvider(boolean enabled) {
        return new LogsdbIndexModeSettingsProvider(Settings.builder().put("cluster.logsdb.enabled", enabled).build());
    }

    @Before
    public void setup() throws Exception {
        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(any())).thenReturn(true);
        var licenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        licenseService.setLicenseState(licenseState);
        var mockLicenseService = mock(LicenseService.class);
        License license = createEnterpriseLicense();
        when(mockLicenseService.getLicense()).thenReturn(license);
        syntheticSourceLicenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        syntheticSourceLicenseService.setLicenseState(licenseState);
        syntheticSourceLicenseService.setLicenseService(mockLicenseService);

        provider = new SyntheticSourceIndexSettingsProvider(syntheticSourceLicenseService, im -> {
            newMapperServiceCounter.incrementAndGet();
            return MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), im.getSettings(), im.getIndex().getName());
        }, getLogsdbIndexModeSettingsProvider(false), IndexVersion::current);
        newMapperServiceCounter.set(0);
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
            assertThat(newMapperServiceCounter.get(), equalTo(1));
            assertWarnings(SourceFieldMapper.DEPRECATION_WARNING);
        }
        {
            String mapping;
            boolean withSourceMode = randomBoolean();
            if (withSourceMode) {
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
            assertThat(newMapperServiceCounter.get(), equalTo(2));
            if (withSourceMode) {
                assertWarnings(SourceFieldMapper.DEPRECATION_WARNING);
            }
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
            assertThat(newMapperServiceCounter.get(), equalTo(0));
        }
        {
            Settings settings = Settings.builder().put("index.mode", "logsdb").build();
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, settings, List.of());
            assertTrue(result);
            assertThat(newMapperServiceCounter.get(), equalTo(0));
        }
        {
            boolean result = provider.newIndexHasSyntheticSourceUsage(indexName, null, Settings.EMPTY, List.of());
            assertFalse(result);
            assertThat(newMapperServiceCounter.get(), equalTo(1));
        }
        {
            boolean result = provider.newIndexHasSyntheticSourceUsage(
                indexName,
                null,
                Settings.EMPTY,
                List.of(new CompressedXContent(mapping))
            );
            assertFalse(result);
            assertThat(newMapperServiceCounter.get(), equalTo(2));
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
            assertThat(newMapperServiceCounter.get(), equalTo(1));
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
            assertThat(newMapperServiceCounter.get(), equalTo(2));
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
        assertThat(newMapperServiceCounter.get(), equalTo(0));

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
        assertThat(newMapperServiceCounter.get(), equalTo(0));

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
        assertThat(newMapperServiceCounter.get(), equalTo(0));

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
        assertThat(newMapperServiceCounter.get(), equalTo(0));
    }

    public void testGetAdditionalIndexSettingsDowngradeFromSyntheticSourceFileMatch() throws IOException {
        syntheticSourceLicenseService.setSyntheticSourceFallback(true);
        provider = new SyntheticSourceIndexSettingsProvider(
            syntheticSourceLicenseService,
            im -> MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), im.getSettings(), im.getIndex().getName()),
            getLogsdbIndexModeSettingsProvider(true),
            IndexVersion::current
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
        assertThat(newMapperServiceCounter.get(), equalTo(0));

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
        assertThat(newMapperServiceCounter.get(), equalTo(0));

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
        assertThat(newMapperServiceCounter.get(), equalTo(0));
    }
}
