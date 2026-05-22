/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ColumnarLogsdbIndexModeTests extends ESTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("logsdb_columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
    }

    public void testColumnarLogsdbFromString() {
        assertThat(IndexMode.fromString("logsdb_columnar"), equalTo(IndexMode.LOGSDB_COLUMNAR));
        assertThat(IndexMode.fromString("LOGSDB_COLUMNAR"), equalTo(IndexMode.LOGSDB_COLUMNAR));
    }

    public void testColumnarLogsdbSerialization() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexMode.writeTo(IndexMode.LOGSDB_COLUMNAR, out);
            try (var in = out.bytes().streamInput()) {
                assertThat(IndexMode.readFrom(in), equalTo(IndexMode.LOGSDB_COLUMNAR));
            }
        }
    }

    public void testColumnarLogsdbSerializationFailsOnOlderTransportVersion() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(IndexMode.COLUMNAR_INDEX_MODES_ADDED));
            IOException e = expectThrows(IOException.class, () -> IndexMode.writeTo(IndexMode.LOGSDB_COLUMNAR, out));
            assertThat(e.getMessage(), containsString("cannot serialize index mode [logsdb_columnar]"));
        }
    }

    public void testColumnarLogsdbIndexModeSetting() {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName()).build();
        assertThat(IndexSettings.MODE.get(settings), equalTo(IndexMode.LOGSDB_COLUMNAR));
    }

    public void testColumnarLogsdbDefaultSourceMode() {
        assertThat(IndexMode.LOGSDB_COLUMNAR.defaultSourceMode(), equalTo(SourceFieldMapper.Mode.SYNTHETIC));
    }

    public void testColumnarLogsdbGetName() {
        assertThat(IndexMode.LOGSDB_COLUMNAR.getName(), equalTo("logsdb_columnar"));
        assertThat(IndexMode.LOGSDB_COLUMNAR.toString(), equalTo("logsdb_columnar"));
    }

    public void testColumnarLogsdbShouldValidateTimestamp() {
        assertThat(IndexMode.LOGSDB_COLUMNAR.shouldValidateTimestamp(), equalTo(false));
    }

    public void testColumnarLogsdbTimestampBound() {
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta(
            "test",
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName()).build()
        );
        assertThat(IndexMode.LOGSDB_COLUMNAR.getTimestampBound(metadata), equalTo(null));
    }

    public void testDefaultHostNameSortField() {
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", buildSettings());
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        boolean sortOnHostName = randomBoolean();
        final IndexSettings settings = new IndexSettings(
            metadata,
            Settings.builder().put(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.getKey(), sortOnHostName).build()
        );
        assertThat(settings.getIndexSortConfig().hasPrimarySortOnField("host.name"), equalTo(sortOnHostName));
    }

    public void testDefaultHostNameSortFieldAndMapping() {
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", buildSettings());
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        final IndexSettings settings = new IndexSettings(
            metadata,
            Settings.builder()
                .put(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.getKey(), true)
                .put(IndexSettings.LOGSDB_ADD_HOST_NAME_FIELD.getKey(), true)
                .build()
        );
        assertThat(settings.getIndexSortConfig().hasPrimarySortOnField("host.name"), equalTo(true));
        assertThat(IndexMode.LOGSDB_COLUMNAR.getDefaultMapping(settings).string(), containsString("host.name"));
    }

    public void testDefaultHostNameSortFieldBwc() {
        final IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(
                indexSettings(IndexVersionUtils.getPreviousVersion(IndexVersions.LOGSB_OPTIONAL_SORTING_ON_HOST_NAME), 1, 1).put(
                    buildSettings()
                )
            )
            .build();
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(settings.getIndexSortConfig().hasPrimarySortOnField("host.name"), equalTo(true));
    }

    public void testDefaultHostNameSortWithOrder() {
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", buildSettings());
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new IndexSettings(
                metadata,
                Settings.builder()
                    .put(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.getKey(), randomBoolean())
                    .put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), "desc")
                    .build()
            )
        );
        assertEquals("setting [index.sort.order] requires [index.sort.field] to be configured", exception.getMessage());
    }

    public void testDefaultHostNameSortWithMode() {
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", buildSettings());
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new IndexSettings(
                metadata,
                Settings.builder()
                    .put(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.getKey(), randomBoolean())
                    .put(IndexSortConfig.INDEX_SORT_MODE_SETTING.getKey(), "MAX")
                    .build()
            )
        );
        assertEquals("setting [index.sort.mode] requires [index.sort.field] to be configured", exception.getMessage());
    }

    public void testDefaultHostNameSortWithMissing() {
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", buildSettings());
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new IndexSettings(
                metadata,
                Settings.builder()
                    .put(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.getKey(), randomBoolean())
                    .put(IndexSortConfig.INDEX_SORT_MISSING_SETTING.getKey(), "_first")
                    .build()
            )
        );
        assertEquals("setting [index.sort.missing] requires [index.sort.field] to be configured", exception.getMessage());
    }

    public void testCustomSortField() {
        final Settings sortSettings = Settings.builder()
            .put(buildSettings())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "agent_id")
            .build();
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", sortSettings);
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(settings.getMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        assertThat(getIndexSetting(settings, IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey()), equalTo("agent_id"));
        assertThat(settings.getIndexSortConfig().hasPrimarySortOnField("host.name"), equalTo(false));
        assertThat(IndexMode.LOGSDB_COLUMNAR.getDefaultMapping(settings).string(), not(containsString("host")));
    }

    public void testSortMode() {
        final Settings sortSettings = Settings.builder()
            .put(buildSettings())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "agent_id")
            .put(IndexSortConfig.INDEX_SORT_MODE_SETTING.getKey(), "max")
            .build();
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", sortSettings);
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(settings.getMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        assertThat("agent_id", equalTo(getIndexSetting(settings, IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey())));
        assertThat("max", equalTo(getIndexSetting(settings, IndexSortConfig.INDEX_SORT_MODE_SETTING.getKey())));
    }

    public void testSortOrder() {
        final Settings sortSettings = Settings.builder()
            .put(buildSettings())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "agent_id")
            .put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), "desc")
            .build();
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", sortSettings);
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(settings.getMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        assertThat("agent_id", equalTo(getIndexSetting(settings, IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey())));
        assertThat("desc", equalTo(getIndexSetting(settings, IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey())));
    }

    public void testSortMissing() {
        final Settings sortSettings = Settings.builder()
            .put(buildSettings())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "agent_id")
            .put(IndexSortConfig.INDEX_SORT_MISSING_SETTING.getKey(), "_last")
            .build();
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", sortSettings);
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(settings.getMode(), equalTo(IndexMode.LOGSDB_COLUMNAR));
        assertThat("agent_id", equalTo(getIndexSetting(settings, IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey())));
        assertThat("_last", equalTo(getIndexSetting(settings, IndexSortConfig.INDEX_SORT_MISSING_SETTING.getKey())));
    }

    public void testIgnoreMalformedDefaultsToTrue() {
        Settings settings = IndexSettingsTests.newIndexMeta("test", buildSettings()).getSettings();
        assertTrue(FieldMapper.IGNORE_MALFORMED_SETTING.get(settings));
    }

    public void testIgnoreMalformedDefaultsToFalseOnOldVersion() {
        Settings settings = IndexSettingsTests.newIndexMeta("test", buildSettings(), IndexVersions.LENIENT_UPDATEABLE_SYNONYMS)
            .getSettings();
        assertFalse(FieldMapper.IGNORE_MALFORMED_SETTING.get(settings));
    }

    public void testIgnoreDynamicBeyondLimitDefaultsToTrue() {
        Settings settings = IndexSettingsTests.newIndexMeta("test", buildSettings()).getSettings();
        assertTrue(MapperService.INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.get(settings));
    }

    public void testIgnoreDynamicBeyondLimitDefaultsToFalseOnOldVersion() {
        Settings settings = IndexSettingsTests.newIndexMeta("test", buildSettings(), IndexVersions.ENABLE_IGNORE_ABOVE_LOGSDB)
            .getSettings();
        assertFalse(MapperService.INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.get(settings));
    }

    private Settings buildSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName()).build();
    }

    private String getIndexSetting(final IndexSettings settings, final String name) {
        return settings.getIndexMetadata().getSettings().get(name);
    }
}
