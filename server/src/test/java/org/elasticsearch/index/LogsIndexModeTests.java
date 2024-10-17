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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class LogsIndexModeTests extends ESTestCase {
    public void testLogsIndexModeSetting() {
        assertThat(IndexSettings.MODE.get(buildSettings()), equalTo(IndexMode.LOGSDB));
    }

    public void testDefaultHostNameSortField() {
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", buildSettings());
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB));
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(settings.getIndexSortConfig().hasPrimarySortOnField("host.name"), equalTo(true));
        assertThat(IndexMode.LOGSDB.getDefaultMapping(settings).string(), containsString("host.name"));
    }

    public void testCustomSortField() {
        final Settings sortSettings = Settings.builder()
            .put(buildSettings())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "agent_id")
            .build();
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", sortSettings);
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB));
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(settings.getMode(), equalTo(IndexMode.LOGSDB));
        assertThat(getIndexSetting(settings, IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey()), equalTo("agent_id"));
        assertThat(settings.getIndexSortConfig().hasPrimarySortOnField("host.name"), equalTo(false));
        assertThat(IndexMode.LOGSDB.getDefaultMapping(settings).string(), not(containsString("host")));
    }

    public void testSortMode() {
        final Settings sortSettings = Settings.builder()
            .put(buildSettings())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "agent_id")
            .put(IndexSortConfig.INDEX_SORT_MODE_SETTING.getKey(), "max")
            .build();
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", sortSettings);
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB));
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(settings.getMode(), equalTo(IndexMode.LOGSDB));
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
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB));
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(settings.getMode(), equalTo(IndexMode.LOGSDB));
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
        assertThat(metadata.getIndexMode(), equalTo(IndexMode.LOGSDB));
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(settings.getMode(), equalTo(IndexMode.LOGSDB));
        assertThat("agent_id", equalTo(getIndexSetting(settings, IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey())));
        assertThat("_last", equalTo(getIndexSetting(settings, IndexSortConfig.INDEX_SORT_MISSING_SETTING.getKey())));
    }

    private Settings buildSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()).build();
    }

    private String getIndexSetting(final IndexSettings settings, final String name) {
        return settings.getIndexMetadata().getSettings().get(name);
    }
}
