/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamIndexSettingsProviderTests extends ESTestCase {

    private static final TimeValue DEFAULT_LOOK_BACK_TIME = TimeValue.timeValueHours(2); // default
    private static final TimeValue DEFAULT_LOOK_AHEAD_TIME = TimeValue.timeValueMinutes(30); // default

    DataStreamIndexSettingsProvider provider;
    private boolean indexDimensionsTsidStrategyEnabledSetting;
    private boolean expectedIndexDimensionsTsidOptimizationEnabled;
    private IndexVersion indexVersion;

    @Before
    public void setup() {
        provider = new DataStreamIndexSettingsProvider(
            im -> MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), im.getSettings(), im.getIndex().getName())
        );
        indexVersion = randomBoolean()
            ? IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.TSID_CREATED_DURING_ROUTING)
            : IndexVersionUtils.randomVersionBetween(random(), IndexVersions.TSID_CREATED_DURING_ROUTING, IndexVersion.current());
        indexDimensionsTsidStrategyEnabledSetting = usually();
        expectedIndexDimensionsTsidOptimizationEnabled = indexDimensionsTsidStrategyEnabledSetting
            && indexVersion.onOrAfter(IndexVersions.TSID_CREATED_DURING_ROUTING);
    }

    public void testGetAdditionalIndexSettings() throws Exception {
        ProjectMetadata projectMetadata = emptyProject();
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Settings settings = Settings.builder()
            .put("index.dimensions_tsid_strategy_enabled", indexDimensionsTsidStrategyEnabledSetting)
            .build();
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
                        },
                        "field4": {
                            "type": "long",
                            "time_series_dimension": true
                        },
                        "field5": {
                            "type": "ip",
                            "time_series_dimension": true
                        },
                        "field6": {
                            "type": "boolean",
                            "time_series_dimension": true
                        }
                    }
                }
            }
            """;
        Settings.Builder additionalSettings = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.TIME_SERIES,
            projectMetadata,
            now,
            settings,
            List.of(new CompressedXContent(mapping)),
            indexVersion,
            additionalSettings
        );
        Settings result = additionalSettings.build();
        // The index.time_series.end_time setting requires index.mode to be set to time_series adding it here so that we read this setting:
        // (in production the index.mode setting is usually provided in an index or component template)
        result = builder().put(result).put("index.mode", "time_series").build();
        assertThat(result.size(), equalTo(4));
        assertThat(IndexSettings.MODE.get(result), equalTo(IndexMode.TIME_SERIES));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        if (expectedIndexDimensionsTsidOptimizationEnabled) {
            assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), containsInAnyOrder("field3", "field4", "field5", "field6"));
            assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), empty());
        } else {
            assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), containsInAnyOrder("field3", "field4", "field5", "field6"));
            assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), empty());
        }
    }

    public void testGetAdditionalIndexSettingsIndexRoutingPathAlreadyDefined() throws Exception {
        ProjectMetadata projectMetadata = emptyProject();
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Settings settings = builder().putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field2").build();
        String mapping = """
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
                        },
                        "field3": {
                            "type": "keyword",
                            "time_series_dimension": true
                        }
                    }
                }
            }
            """;
        Settings.Builder additionalSettings = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.TIME_SERIES,
            projectMetadata,
            now,
            settings,
            List.of(new CompressedXContent(mapping)),
            indexVersion,
            additionalSettings
        );
        Settings result = additionalSettings.build();
        // The index.time_series.end_time setting requires index.mode to be set to time_series adding it here so that we read this setting:
        // (in production the index.mode setting is usually provided in an index or component template)
        result = builder().put(result).put("index.mode", "time_series").build();
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(IndexSettings.MODE.getKey()), equalTo("time_series"));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
    }

    public void testGetAdditionalIndexSettingsMappingsMerging() throws Exception {
        ProjectMetadata projectMetadata = emptyProject();
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Settings settings = Settings.builder()
            .put("index.dimensions_tsid_strategy_enabled", indexDimensionsTsidStrategyEnabledSetting)
            .build();
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
        Settings.Builder additionalSettings = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.TIME_SERIES,
            projectMetadata,
            now,
            settings,
            List.of(new CompressedXContent(mapping1), new CompressedXContent(mapping2), new CompressedXContent(mapping3)),
            indexVersion,
            additionalSettings
        );
        Settings result = additionalSettings.build();
        // The index.time_series.end_time setting requires index.mode to be set to time_series adding it here so that we read this setting:
        // (in production the index.mode setting is usually provided in an index or component template)
        result = builder().put(result).put("index.mode", "time_series").build();
        assertThat(result.size(), equalTo(4));
        assertThat(IndexSettings.MODE.get(result), equalTo(IndexMode.TIME_SERIES));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        if (expectedIndexDimensionsTsidOptimizationEnabled) {
            assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), containsInAnyOrder("field1", "field3"));
            assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), empty());
        } else {
            assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), containsInAnyOrder("field1", "field3"));
            assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), empty());
        }
    }

    public void testGetAdditionalIndexSettingsNoMappings() {
        ProjectMetadata projectMetadata = emptyProject();
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Settings settings = Settings.EMPTY;
        Settings.Builder additionalSettings = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.TIME_SERIES,
            projectMetadata,
            now,
            settings,
            List.of(),
            indexVersion,
            additionalSettings
        );
        Settings result = additionalSettings.build();
        // The index.time_series.end_time setting requires index.mode to be set to time_series adding it here so that we read this setting:
        // (in production the index.mode setting is usually provided in an index or component template)
        result = builder().put(result).put("index.mode", "time_series").build();
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(IndexSettings.MODE.getKey()), equalTo("time_series"));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
    }

    public void testGetAdditionalIndexSettingsLookAheadTime() throws Exception {
        ProjectMetadata projectMetadata = emptyProject();
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookAheadTime = TimeValue.timeValueMinutes(30);
        Settings settings = builder().put("index.mode", "time_series").put("index.look_ahead_time", lookAheadTime.getStringRep()).build();
        Settings.Builder additionalSettings = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.TIME_SERIES,
            projectMetadata,
            now,
            settings,
            List.of(new CompressedXContent("{}")),
            indexVersion,
            additionalSettings
        );
        Settings result = additionalSettings.build();
        // The index.time_series.end_time setting requires index.mode to be set to time_series adding it here so that we read this setting:
        // (in production the index.mode setting is usually provided in an index or component template)
        result = builder().put(result).put("index.mode", "time_series").build();
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(IndexSettings.MODE.getKey()), equalTo("time_series"));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
    }

    public void testGetAdditionalIndexSettingsLookBackTime() throws Exception {
        ProjectMetadata projectMetadata = emptyProject();
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookBackTime = TimeValue.timeValueHours(12);
        Settings settings = builder().put("index.mode", "time_series").put("index.look_back_time", lookBackTime.getStringRep()).build();
        Settings.Builder additionalSettings = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.TIME_SERIES,
            projectMetadata,
            now,
            settings,
            List.of(new CompressedXContent("{}")),
            indexVersion,
            additionalSettings
        );
        Settings result = additionalSettings.build();
        // The index.time_series.end_time setting requires index.mode to be set to time_series adding it here so that we read this setting:
        // (in production the index.mode setting is usually provided in an index or component template)
        result = builder().put(result).put("index.mode", "time_series").build();
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(IndexSettings.MODE.getKey()), equalTo("time_series"));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookBackTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
    }

    public void testGetAdditionalIndexSettingsDataStreamAlreadyCreated() throws Exception {
        String dataStreamName = "logs-app1";
        TimeValue lookAheadTime = TimeValue.timeValueMinutes(30);

        Instant sixHoursAgo = Instant.now().minus(6, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS);
        Instant currentEnd = sixHoursAgo.plusMillis(lookAheadTime.getMillis());
        ProjectMetadata projectMetadata = DataStreamTestHelper.getProjectWithDataStream(
            randomProjectIdOrDefault(),
            dataStreamName,
            List.of(new Tuple<>(sixHoursAgo, currentEnd))
        );

        Instant now = sixHoursAgo.plus(6, ChronoUnit.HOURS);
        Settings settings = Settings.EMPTY;
        Settings.Builder additionalSettings = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.TIME_SERIES,
            projectMetadata,
            now,
            settings,
            List.of(new CompressedXContent("{}")),
            indexVersion,
            additionalSettings
        );
        var result = additionalSettings.build();
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
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(
            DataStreamTestHelper.getProjectWithDataStreams(
                List.of(Tuple.tuple(dataStreamName, 1)),
                List.of(),
                twoHoursAgo.toEpochMilli(),
                builder().build(),
                1
            )
        );
        DataStream ds = projectBuilder.dataStream(dataStreamName);
        projectBuilder.put(ds.copy().setIndexMode(IndexMode.TIME_SERIES).build());
        ProjectMetadata projectMetadata = projectBuilder.build();

        Instant now = twoHoursAgo.plus(2, ChronoUnit.HOURS);
        Settings settings = Settings.EMPTY;
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> provider.provideAdditionalSettings(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                dataStreamName,
                IndexMode.TIME_SERIES,
                projectMetadata,
                now,
                settings,
                null,
                indexVersion,
                builder()
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
        ProjectMetadata projectMetadata = emptyProject();
        String dataStreamName = "logs-app1";

        Settings settings = Settings.EMPTY;
        Settings.Builder additionalSettings = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            null,
            projectMetadata,
            Instant.ofEpochMilli(1L),
            settings,
            null,
            indexVersion,
            additionalSettings
        );
        Settings result = additionalSettings.build();
        assertThat(result.size(), equalTo(0));
    }

    public void testGetAdditionalIndexSettingsMigrateToTsdb() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String dataStreamName = "logs-app1";
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).build();
        DataStream existingDataStream = newInstance(dataStreamName, List.of(idx.getIndex()));
        ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .dataStreams(Map.of(dataStreamName, existingDataStream), Map.of())
            .build();

        Settings settings = Settings.EMPTY;
        Settings.Builder additionalSettings = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            dataStreamName,
            IndexMode.TIME_SERIES,
            projectMetadata,
            now,
            settings,
            List.of(),
            indexVersion,
            additionalSettings
        );
        Settings result = additionalSettings.build();
        // The index.time_series.end_time setting requires index.mode to be set to time_series adding it here so that we read this setting:
        // (in production the index.mode setting is usually provided in an index or component template)
        result = builder().put(result).put("index.mode", "time_series").build();
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(IndexSettings.MODE.getKey()), equalTo("time_series"));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
    }

    public void testGetAdditionalIndexSettingsDowngradeFromTsdb() {
        String dataStreamName = "logs-app1";
        Instant twoHoursAgo = Instant.now().minus(4, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);
        ProjectMetadata projectMetadata = DataStreamTestHelper.getProjectWithDataStreams(
            List.of(Tuple.tuple(dataStreamName, 1)),
            List.of(),
            twoHoursAgo.toEpochMilli(),
            builder().build(),
            1
        );

        Settings.Builder additionalSettings = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            dataStreamName,
            null,
            projectMetadata,
            Instant.ofEpochMilli(1L),
            Settings.EMPTY,
            List.of(),
            indexVersion,
            additionalSettings
        );
        Settings result = additionalSettings.build();
        assertThat(result.size(), equalTo(0));
    }

    public void testGenerateRoutingPathFromDynamicTemplate() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
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
        assertThat(result.size(), equalTo(4));
        assertThat(IndexSettings.MODE.get(result), equalTo(IndexMode.TIME_SERIES));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), containsInAnyOrder("host.id", "prometheus.labels.*"));
        assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), empty());
    }

    public void testGenerateRoutingPathFromDynamicTemplateWithMultiplePathMatchEntries() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
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
        assertThat(result.size(), equalTo(4));
        assertThat(IndexSettings.MODE.get(result), equalTo(IndexMode.TIME_SERIES));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        assertThat(
            IndexMetadata.INDEX_ROUTING_PATH.get(result),
            containsInAnyOrder("host.id", "xprometheus.labels.*", "yprometheus.labels.*")
        );
        assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), empty());
    }

    public void testGenerateRoutingPathFromDynamicTemplateWithMultiplePathMatchEntriesMultiFields() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String mapping = """
            {
                "_doc": {
                    "dynamic_templates": [
                        {
                            "labels": {
                                "path_match": ["xprometheus.labels.*", "yprometheus.labels.*"],
                                "mapping": {
                                    "type": "keyword",
                                    "time_series_dimension": true,
                                    "fields": {
                                      "text": {
                                        "type": "text"
                                      }
                                    }
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
        assertThat(result.size(), equalTo(4));
        assertThat(IndexSettings.MODE.get(result), equalTo(IndexMode.TIME_SERIES));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        assertThat(
            IndexMetadata.INDEX_ROUTING_PATH.get(result),
            containsInAnyOrder("host.id", "xprometheus.labels.*", "yprometheus.labels.*")
        );
        assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), empty());
    }

    public void testGenerateRoutingPathFromDynamicTemplate_templateWithNoPathMatch() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
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
        assertThat(result.size(), equalTo(4));
        assertThat(IndexSettings.MODE.get(result), equalTo(IndexMode.TIME_SERIES));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), containsInAnyOrder("host.id", "prometheus.labels.*"));
        assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), empty());
    }

    public void testGenerateNonDimensionDynamicTemplate() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String mapping = """
            {
                "_doc": {
                    "dynamic_templates": [
                        {
                            "strings_as_keyword": {
                                "match_mapping_type": "string",
                                "mapping": {
                                    "type": "keyword",
                                    "ignore_above": 1024,
                                    "time_series_dimension": false
                                }
                            }
                        }
                    ],
                    "properties": {
                        "host.id": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "another_field": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
        Settings result = generateTsdbSettings(mapping, now);
        assertThat(result.size(), equalTo(4));
        assertThat(IndexSettings.MODE.get(result), equalTo(IndexMode.TIME_SERIES));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        if (expectedIndexDimensionsTsidOptimizationEnabled) {
            assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), containsInAnyOrder("host.id"));
            assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), empty());
        } else {
            assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), containsInAnyOrder("host.id"));
            assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), empty());
        }
    }

    public void testGenerateRoutingPathFromDynamicTemplate_nonKeywordTemplate() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
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
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), empty());
    }

    public void testGenerateRoutingPathFromPassThroughObject() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "labels": {
                            "type": "passthrough",
                            "time_series_dimension": true,
                            "priority": 2,
                             "properties": {
                                "label1": {
                                    "type": "keyword"
                                }
                             }
                        },
                        "metrics": {
                            "type": "passthrough",
                            "priority": 1
                        },
                        "another_field": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
        Settings result = generateTsdbSettings(mapping, now);
        assertThat(result.size(), equalTo(4));
        assertThat(IndexSettings.MODE.get(result), equalTo(IndexMode.TIME_SERIES));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        if (expectedIndexDimensionsTsidOptimizationEnabled) {
            assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), containsInAnyOrder("labels.*"));
            assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), empty());
        } else {
            assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), containsInAnyOrder("labels.*"));
            assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), empty());
        }
    }

    public void testDynamicTemplatePrecedence() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String mapping = """
            {
                "_doc": {
                    "dynamic_templates": [
                        {
                            "no_dimension_labels": {
                                "path_match": "labels.host_ip",
                                "mapping": {
                                    "type": "keyword"
                                }
                            }
                        },
                        {
                            "labels": {
                                "path_match": "labels.*",
                                "mapping": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        }
                    ]
                }
            }
            """;
        Settings result = generateTsdbSettings(mapping, now);
        assertThat(result.size(), equalTo(4));
        assertThat(IndexSettings.MODE.get(result), equalTo(IndexMode.TIME_SERIES));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(DEFAULT_LOOK_BACK_TIME.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(DEFAULT_LOOK_AHEAD_TIME.getMillis())));
        // labels.host_ip is not a dimension because it matches the first template which does not have time_series_dimension:true
        // we can't use index.dimensions as it would add non-dimension fields to the tsid
        assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), empty());
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), containsInAnyOrder("labels.*"));
    }

    public void testAddNewDimension() throws Exception {
        String newMapping = """
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
        Settings result = onUpdateMappings("field1", "field1", newMapping);
        assertThat(result.size(), equalTo(1));
        assertThat(IndexMetadata.INDEX_DIMENSIONS.get(result), containsInAnyOrder("field1", "field2"));
    }

    public void testAddNewDimensionIndexDimensionsUnset() throws Exception {
        String newMapping = """
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
        Settings result = onUpdateMappings("field1", null, newMapping);
        assertThat(result.size(), equalTo(0));
    }

    public void testAddPassthroughChildField() throws Exception {
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "labels": {
                            "type": "passthrough",
                            "time_series_dimension": true,
                            "priority": 2,
                             "properties": {
                                "label1": {
                                    "type": "keyword"
                                }
                             }
                        },
                        "metrics": {
                            "type": "passthrough",
                            "priority": 1
                        },
                        "another_field": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
        // the new labels.label1 field already matches labels.*, so no change
        Settings result = onUpdateMappings("labels.*", "labels.*", mapping);
        assertThat(result.size(), equalTo(0));
    }

    public void testAddDynamicTemplate() throws Exception {
        String mapping = """
            {
                "_doc": {
                    "dynamic_templates": [
                        {
                            "string_as_dimensions": {
                                "match_mapping_type": "string",
                                "mapping": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        }
                    ],
                    "properties": {
                        "labels": {
                            "type": "passthrough",
                            "time_series_dimension": true,
                            "priority": 1
                        }
                    }
                }
            }
            """;
        // we don't support index.dimensions with dynamic templates so we'll unset index.dimensions
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> onUpdateMappings(null, "labels.*", mapping)
        );
        assertThat(
            exception.getMessage(),
            equalTo(
                "Cannot add dynamic templates that define dimension fields on an existing index with index.dimensions. "
                    + "Please change the index template and roll over the data stream "
                    + "instead of modifying the mappings of the backing indices."
            )
        );
    }

    private Settings generateTsdbSettings(String mapping, Instant now) throws IOException {
        ProjectMetadata projectMetadata = emptyProject();
        String dataStreamName = "logs-app1";
        Settings settings = Settings.builder()
            .put("index.dimensions_tsid_strategy_enabled", indexDimensionsTsidStrategyEnabledSetting)
            .build();

        Settings.Builder additionalSettings = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.TIME_SERIES,
            projectMetadata,
            now,
            settings,
            List.of(new CompressedXContent(mapping)),
            indexVersion,
            additionalSettings
        );
        var result = additionalSettings.build();
        // The index.time_series.end_time setting requires index.mode to be set to time_series adding it here so that we read this setting:
        // (in production the index.mode setting is usually provided in an index or component template)
        return builder().put(result).put("index.mode", "time_series").build();
    }

    private Settings onUpdateMappings(String routingPath, String dimensions, String newMapping) throws IOException {
        String dataStreamName = "logs-app1";
        Settings.Builder currentSettings = Settings.builder()
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), dimensions)
            .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES);

        IndexMetadata im = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            .settings(currentSettings)
            .build();

        DocumentMapper documentMapper;
        MapperService mapperService = MapperTestUtils.newMapperService(
            xContentRegistry(),
            createTempDir(),
            im.getSettings(),
            im.getIndex().getName()
        );
        try (mapperService) {
            mapperService.merge(
                MapperService.SINGLE_MAPPING_NAME,
                List.of(new CompressedXContent(newMapping)),
                MapperService.MergeReason.INDEX_TEMPLATE
            );
            documentMapper = mapperService.documentMapper();
        }
        Settings.Builder additionalSettings = builder();
        provider.onUpdateMappings(im, documentMapper, additionalSettings);
        return additionalSettings.build();
    }

}
