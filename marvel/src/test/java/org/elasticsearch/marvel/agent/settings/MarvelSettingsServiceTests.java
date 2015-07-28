/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.settings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

public class MarvelSettingsServiceTests extends ElasticsearchTestCase {

    @Test
    public void testMarvelSettingService() {
        MarvelSettingsService service = new MarvelSettingsService(Settings.EMPTY);

        TimeValue indexStatsTimeout = service.indexStatsTimeout();
        assertNotNull(indexStatsTimeout);

        String[] indices = service.indices();
        assertNotNull(indices);

        TimeValue updatedIndexStatsTimeout = TimeValue.timeValueSeconds(60L);
        String[] updatedIndices = new String[]{"index-0", "index-1"};

        Settings settings = settingsBuilder()
                .put(service.indexStatsTimeout.getName(), updatedIndexStatsTimeout)
                .put(service.indices.getName(), Strings.arrayToCommaDelimitedString(updatedIndices))
                .build();

        service.onRefreshSettings(settings);

        assertThat(service.indexStatsTimeout(), equalTo(updatedIndexStatsTimeout));
        assertArrayEquals(service.indices(), updatedIndices);
    }
}
