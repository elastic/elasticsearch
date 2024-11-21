/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LegacyLicenceIntegrationTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateLogsDPPlugin.class);
    }

    public void testSyntheticSourceUsageDisallowed() {
        String indexName = "test";
        var settings = Settings.builder().put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic").build();
        createIndex(indexName, settings);
        var response = admin().indices().getSettings(new GetSettingsRequest().indices(indexName)).actionGet();
        assertThat(
            response.getIndexToSettings().get(indexName).get(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey()),
            equalTo("STORED")
        );
    }

    public void testSyntheticSourceUsageAllowed() {
        String indexName = ".profiling-stacktraces";
        var settings = Settings.builder().put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic").build();
        createIndex(indexName, settings);
        var response = admin().indices().getSettings(new GetSettingsRequest().indices(indexName)).actionGet();
        assertThat(
            response.getIndexToSettings().get(indexName).get(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey()),
            equalTo("synthetic")
        );
    }
}
