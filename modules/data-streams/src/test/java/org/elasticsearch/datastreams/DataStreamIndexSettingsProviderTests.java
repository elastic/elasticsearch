/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFirstBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.datastreams.DataStreamIndexSettingsProvider.FORMATTER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamIndexSettingsProviderTests extends ESTestCase {

    private static final TimeValue DEFAULT_LOOK_AHEAD_TIME = TimeValue.timeValueHours(2); // default

    DataStreamIndexSettingsProvider provider;

    @Before
    public void setup() {
        provider = new DataStreamIndexSettingsProvider(
            im -> MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), im.getSettings(), im.getIndex().getName())
        );
    }

    public void testGetAdditionalIndexSettings() throws Exception {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Settings settings = Settings.EMPTY;
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "field1": {
                            "type": "long"
                        },
                        "field2": {
                            "type": "keyword"
                        },
                        "field3": {
                            "type": "keyword",
                            "time_series_dimension": true
                        }
                    }
                }
            }
            """;
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            true,
            metadata,
            now,
            settings,
            List.of(new CompressedXContent(mapping))
        );
        assertThat(result.size(), equalTo(3));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), contains("field3"));
    }

    public void testGetAdditionalIndexSettingsIndexRoutingPathAlreadyDefined() throws Exception {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookAheadTime = TimeValue.timeValueHours(2); // default
        Settings settings = builder().putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field2").build();
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "field1": {
                            "type": "keyword"
                            "time_series_dimension": true
                        },
                        "field2": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "field3": {
                            "type": "keyword",
                            "time_series_dimension": true
                        }
                    }
                }
            }
            """;
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            true,
            metadata,
            now,
            settings,
            List.of(new CompressedXContent(mapping))
        );
        assertThat(result.size(), equalTo(2));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
    }

    public void testGetAdditionalIndexSettingsMappingsMerging() throws Exception {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookAheadTime = TimeValue.timeValueHours(2); // default
        Settings settings = Settings.EMPTY;
        String mapping1 = """
            {
                "_doc": {
                    "properties": {
                        "field1": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "field2": {
                            "type": "keyword",
                            "time_series_dimension": true
                        }
                    }
                }
            }
            """;
        String mapping2 = """
            {
                "_doc": {
                    "properties": {
                        "field3": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "field4": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
        String mapping3 = """
            {
                "_doc": {
                    "properties": {
                        "field2": {
                            "type": "keyword",
                            "time_series_dimension": false
                        },
                        "field5": {
                            "type": "long"
                        }
                    }
                }
            }
            """;
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            true,
            metadata,
            now,
            settings,
            List.of(new CompressedXContent(mapping1), new CompressedXContent(mapping2), new CompressedXContent(mapping3))
        );
        assertThat(result.size(), equalTo(3));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), containsInAnyOrder("field1", "field3"));
    }

    public void testGetAdditionalIndexSettingsNoMappings() {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookAheadTime = TimeValue.timeValueHours(2); // default
        Settings settings = Settings.EMPTY;
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            true,
            metadata,
            now,
            settings,
            List.of()
        );
        assertThat(result.size(), equalTo(2));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
    }

    public void testGetAdditionalIndexSettingsLookAheadTime() throws Exception {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookAheadTime = TimeValue.timeValueMinutes(30);
        Settings settings = builder().put("index.mode", "time_series").put("index.look_ahead_time", lookAheadTime.getStringRep()).build();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            true,
            metadata,
            now,
            settings,
            List.of(new CompressedXContent("{}"))
        );
        assertThat(result.size(), equalTo(2));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
    }

    public void testGetAdditionalIndexSettingsDataStreamAlreadyCreated() throws Exception {
        String dataStreamName = "logs-app1";
        TimeValue lookAheadTime = TimeValue.timeValueHours(2);

        Instant sixHoursAgo = Instant.now().minus(6, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS);
        Instant currentEnd = sixHoursAgo.plusMillis(lookAheadTime.getMillis());
        Metadata metadata = DataStreamTestHelper.getClusterStateWithDataStream(
            dataStreamName,
            List.of(new Tuple<>(sixHoursAgo, currentEnd))
        ).getMetadata();

        Instant now = sixHoursAgo.plus(6, ChronoUnit.HOURS);
        Settings settings = Settings.EMPTY;
        var result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            true,
            metadata,
            now,
            settings,
            List.of(new CompressedXContent("{}"))
        );
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(IndexSettings.TIME_SERIES_START_TIME.getKey()), equalTo(FORMATTER.format(currentEnd)));
        assertThat(
            result.get(IndexSettings.TIME_SERIES_END_TIME.getKey()),
            equalTo(FORMATTER.format(now.plusMillis(lookAheadTime.getMillis())))
        );
    }

    public void testGetAdditionalIndexSettingsDataStreamAlreadyCreatedTimeSettingsMissing() {
        String dataStreamName = "logs-app1";
        Instant twoHoursAgo = Instant.now().minus(4, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);
        Metadata.Builder mb = Metadata.builder(
            DataStreamTestHelper.getClusterStateWithDataStreams(
                List.of(Tuple.tuple(dataStreamName, 1)),
                List.of(),
                twoHoursAgo.toEpochMilli(),
                builder().build(),
                1
            ).getMetadata()
        );
        DataStream ds = mb.dataStream(dataStreamName);
        mb.put(
            new DataStream(
                ds.getName(),
                ds.getIndices(),
                ds.getGeneration(),
                ds.getMetadata(),
                ds.isHidden(),
                ds.isReplicated(),
                ds.isSystem(),
                ds.isAllowCustomRouting(),
                IndexMode.TIME_SERIES,
                ds.getLifecycle()
            )
        );
        Metadata metadata = mb.build();

        Instant now = twoHoursAgo.plus(2, ChronoUnit.HOURS);
        Settings settings = Settings.EMPTY;
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> provider.getAdditionalIndexSettings(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                dataStreamName,
                true,
                metadata,
                now,
                settings,
                null
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                Strings.format(
                    "backing index [%s] in tsdb mode doesn't have the [index.time_series.end_time] index setting",
                    DataStream.getDefaultBackingIndexName(dataStreamName, 1, twoHoursAgo.toEpochMilli())
                )
            )
        );
    }

    public void testGetAdditionalIndexSettingsNonTsdbTemplate() {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Settings settings = Settings.EMPTY;
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            false,
            metadata,
            Instant.ofEpochMilli(1L),
            settings,
            null
        );
        assertThat(result.size(), equalTo(0));
    }

    public void testGetAdditionalIndexSettingsMigrateToTsdb() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String dataStreamName = "logs-app1";
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).build();
        DataStream existingDataStream = newInstance(dataStreamName, List.of(idx.getIndex()));
        Metadata metadata = Metadata.builder().dataStreams(Map.of(dataStreamName, existingDataStream), Map.of()).build();

        Settings settings = Settings.EMPTY;
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            dataStreamName,
            true,
            metadata,
            now,
            settings,
            List.of()
        );
        assertThat(result.size(), equalTo(2));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
    }

    public void testGetAdditionalIndexSettingsDowngradeFromTsdb() {
        String dataStreamName = "logs-app1";
        Instant twoHoursAgo = Instant.now().minus(4, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);
        Metadata.Builder mb = Metadata.builder(
            DataStreamTestHelper.getClusterStateWithDataStreams(
                List.of(Tuple.tuple(dataStreamName, 1)),
                List.of(),
                twoHoursAgo.toEpochMilli(),
                builder().build(),
                1
            ).getMetadata()
        );
        Metadata metadata = mb.build();

        Settings settings = Settings.EMPTY;
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            dataStreamName,
            false,
            metadata,
            Instant.ofEpochMilli(1L),
            settings,
            List.of()
        );
        assertThat(result.size(), equalTo(0));
    }

    public void testGenerateRoutingPathFromDynamicTemplate() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookAheadTime = TimeValue.timeValueHours(2); // default
        String mapping = """
            {
                "_doc": {
                    "dynamic_templates": [
                        {
                            "labels": {
                                "path_match": "prometheus.labels.*",
                                "mapping": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        }
                    ],
                    "properties": {
                        "host": {
                            "properties": {
                                "id": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        },
                        "another_field": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
        Settings result = generateTsdbSettings(mapping, now);
        assertThat(result.size(), equalTo(3));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), containsInAnyOrder("host.id", "prometheus.labels.*"));
    }

    public void testGenerateRoutingPathFromDynamicTemplateWithMultiplePathMatchEntries() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookAheadTime = TimeValue.timeValueHours(2); // default
        String mapping = """
            {
                "_doc": {
                    "dynamic_templates": [
                        {
                            "labels": {
                                "path_match": ["xprometheus.labels.*", "yprometheus.labels.*"],
                                "mapping": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        }
                    ],
                    "properties": {
                        "host": {
                            "properties": {
                                "id": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        },
                        "another_field": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
        Settings result = generateTsdbSettings(mapping, now);
        assertThat(result.size(), equalTo(3));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
        assertThat(
            IndexMetadata.INDEX_ROUTING_PATH.get(result),
            containsInAnyOrder("host.id", "xprometheus.labels.*", "yprometheus.labels.*")
        );
        List<String> routingPathList = IndexMetadata.INDEX_ROUTING_PATH.get(result);
        assertEquals(3, routingPathList.size());
    }

    public void testGenerateRoutingPathFromDynamicTemplate_templateWithNoPathMatch() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookAheadTime = TimeValue.timeValueHours(2); // default
        String mapping = """
            {
                "_doc": {
                    "dynamic_templates": [
                        {
                            "labels": {
                                "path_match": "prometheus.labels.*",
                                "mapping": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        },
                        {
                            "strings_as_keyword": {
                                "match_mapping_type": "string",
                                "mapping": {
                                    "type": "keyword",
                                    "ignore_above": 1024
                                }
                            }
                        }
                    ],
                    "properties": {
                        "host": {
                            "properties": {
                                "id": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        },
                        "another_field": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
        Settings result = generateTsdbSettings(mapping, now);
        assertThat(result.size(), equalTo(3));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), containsInAnyOrder("host.id", "prometheus.labels.*"));
    }

    public void testGenerateRoutingPathFromDynamicTemplate_nonKeywordTemplate() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookAheadTime = TimeValue.timeValueHours(2); // default
        String mapping = """
            {
                "_doc": {
                    "dynamic_templates": [
                        {
                            "docker.cpu.core.*.pct": {
                                "path_match": "docker.cpu.core.*.pct",
                                "mapping": {
                                    "coerce": true,
                                    "type": "float"
                                }
                            }
                        },
                        {
                            "labels": {
                                "path_match": "prometheus.labels.*",
                                "mapping": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        }
                    ],
                    "properties": {
                        "host": {
                            "properties": {
                                "id": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        },
                        "another_field": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
        Settings result = generateTsdbSettings(mapping, now);
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), containsInAnyOrder("host.id", "prometheus.labels.*"));
        assertEquals(2, IndexMetadata.INDEX_ROUTING_PATH.get(result).size());
    }

    private Settings generateTsdbSettings(String mapping, Instant now) throws IOException {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";
        Settings settings = Settings.EMPTY;

        return provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            true,
            metadata,
            now,
            settings,
            List.of(new CompressedXContent(mapping))
        );
    }

}
