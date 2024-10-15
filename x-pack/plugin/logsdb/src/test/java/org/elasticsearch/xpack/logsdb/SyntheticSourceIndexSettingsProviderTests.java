/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

public class SyntheticSourceIndexSettingsProviderTests extends ESTestCase {

    private SyntheticSourceIndexSettingsProvider provider;

    @Before
    public void setup() {
        SyntheticSourceLicenseService syntheticSourceLicenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        provider = new SyntheticSourceIndexSettingsProvider(
            syntheticSourceLicenseService,
            im -> MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), im.getSettings(), im.getIndex().getName())
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

}
