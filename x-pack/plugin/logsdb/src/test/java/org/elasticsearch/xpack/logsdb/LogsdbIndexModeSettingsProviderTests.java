/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class LogsdbIndexModeSettingsProviderTests extends ESTestCase {

    private static final String DATA_STREAM_NAME = "logs-apache-production";

    public static final String DEFAULT_MAPPING = """
        {
            "_doc": {
                "properties": {
                    "@timestamp": {
                        "type": "date"
                    },
                    "message": {
                        "type": "keyword"
                    },
                    "host.name": {
                        "type": "keyword"
                    }
                }
            }
        }
        """;

    public void testLogsDbDisabled() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", false).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            null,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertTrue(additionalIndexSettings.isEmpty());
    }

    public void testOnIndexCreation() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            DATA_STREAM_NAME,
            null,
            null,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertTrue(additionalIndexSettings.isEmpty());
    }

    public void testOnExplicitStandardIndex() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            null,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.getName()).build(),
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertTrue(additionalIndexSettings.isEmpty());
    }

    public void testOnExplicitTimeSeriesIndex() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            null,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName()).build(),
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertTrue(additionalIndexSettings.isEmpty());
    }

    public void testNonLogsDataStream() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            "logs",
            null,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertTrue(additionalIndexSettings.isEmpty());
    }

    public void testWithoutLogsComponentTemplate() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            null,
            buildMetadata(List.of("*"), List.of()),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, IndexMode.LOGSDB.getName());
    }

    public void testWithLogsComponentTemplate() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            null,
            buildMetadata(List.of("*"), List.of("logs@settings")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, IndexMode.LOGSDB.getName());
    }

    public void testWithMultipleComponentTemplates() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            null,
            buildMetadata(List.of("*"), List.of("logs@settings", "logs@custom")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, IndexMode.LOGSDB.getName());
    }

    public void testWithCustomComponentTemplatesOnly() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            null,
            buildMetadata(List.of("*"), List.of("logs@custom", "custom-component-template")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, IndexMode.LOGSDB.getName());
    }

    public void testNonMatchingTemplateIndexPattern() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            null,
            buildMetadata(List.of("standard-apache-production"), List.of("logs@settings")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, IndexMode.LOGSDB.getName());
    }

    public void testCaseSensitivity() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME.toUpperCase(Locale.ROOT),
            null,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertTrue(additionalIndexSettings.isEmpty());
    }

    public void testMultipleHyphensInDataStreamName() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME + "-eu",
            null,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, IndexMode.LOGSDB.getName());
    }

    public void testBeforeAndAFterSettingUpdate() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", false).build()
        );

        final Settings beforeSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            null,
            buildMetadata(List.of("*"), List.of("logs@settings")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertTrue(beforeSettings.isEmpty());

        provider.updateClusterIndexModeLogsdbEnabled(true);

        final Settings afterSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            null,
            buildMetadata(List.of("*"), List.of("logs@settings")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(afterSettings, IndexMode.LOGSDB.getName());

        provider.updateClusterIndexModeLogsdbEnabled(false);

        final Settings laterSettings = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            null,
            buildMetadata(List.of("*"), List.of("logs@settings")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertTrue(laterSettings.isEmpty());
    }

    public void testLogsdbRoutingPathOnSortFields() throws Exception {
        var settings = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "host,message")
            .put(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), true)
            .build();
        Settings result = generateLogsdbSettings(settings);
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), contains("host", "message"));
    }

    public void testLogsdbRoutingPathOnSortFieldsFilterTimestamp() throws Exception {
        var settings = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "host,message,@timestamp")
            .put(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), true)
            .build();
        Settings result = generateLogsdbSettings(settings);
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), contains("host", "message"));
    }

    public void testLogsdbRoutingPathOnSortSingleField() throws Exception {
        var settings = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "host")
            .put(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), true)
            .build();
        Exception e = expectThrows(IllegalStateException.class, () -> generateLogsdbSettings(settings));
        assertThat(
            e.getMessage(),
            equalTo(
                "data stream ["
                    + DATA_STREAM_NAME
                    + "] in logsdb mode and with [index.logsdb.route_on_sort_fields] index setting has only 1 sort fields "
                    + "(excluding timestamp), needs at least 2"
            )
        );
    }

    public void testLogsdbExplicitRoutingPathMatchesSortFields() throws Exception {
        var settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB)
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "host,message,@timestamp")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host,message")
            .put(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), true)
            .build();
        Settings result = generateLogsdbSettings(settings);
        assertTrue(result.isEmpty());
    }

    public void testLogsdbExplicitRoutingPathDoesNotMatchSortFields() throws Exception {
        var settings = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "host,message,@timestamp")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host,message,foo")
            .put(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), true)
            .build();
        Exception e = expectThrows(IllegalStateException.class, () -> generateLogsdbSettings(settings));
        assertThat(
            e.getMessage(),
            equalTo(
                "data stream ["
                    + DATA_STREAM_NAME
                    + "] in logsdb mode and with [index.logsdb.route_on_sort_fields] index setting has mismatching sort "
                    + "and routing fields, [index.routing_path:[host, message, foo]], [index.sort.fields:[host, message]]"
            )
        );
    }

    private Settings generateLogsdbSettings(Settings settings) throws IOException {
        Metadata metadata = Metadata.EMPTY_METADATA;
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );
        var result = provider.getAdditionalIndexSettings(
            null,
            DATA_STREAM_NAME,
            IndexMode.LOGSDB,
            metadata,
            Instant.now(),
            settings,
            List.of()
        );
        return builder().put(result).build();
    }

    private static Metadata buildMetadata(final List<String> indexPatterns, final List<String> componentTemplates) throws IOException {
        final Template template = new Template(Settings.EMPTY, new CompressedXContent(DEFAULT_MAPPING), null);
        final ComposableIndexTemplate composableTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(indexPatterns)
            .template(template)
            .componentTemplates(componentTemplates)
            .priority(1_000L)
            .version(1L)
            .build();
        return Metadata.builder()
            .putCustom(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(Map.of("composable", composableTemplate)))
            .build();
    }

    private void assertIndexMode(final Settings settings, final String expectedIndexMode) {
        assertEquals(expectedIndexMode, settings.get(IndexSettings.MODE.getKey()));
    }

}
