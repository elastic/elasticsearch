/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class ConfigTest extends ElasticsearchIntegrationTest {

    class SettingsListener implements ConfigurableComponentListener {
        Settings settings;

        @Override
        public void receiveConfigurationUpdate(Settings settings) {
            this.settings = settings;
        }
    }

    public void testListener() throws Exception {
        TimeValue tv = TimeValue.timeValueMillis(10000);
        Settings oldSettings = ImmutableSettings.builder()
                .put("foo", tv)
                .put("bar", 1)
                .put("baz", true)
                .build();
        ConfigurationService configurationService = new ConfigurationService(oldSettings, client());

        SettingsListener settingsListener = new SettingsListener();
        configurationService.registerListener(settingsListener);
        TimeValue tv2 = TimeValue.timeValueMillis(10);
        XContentBuilder jsonSettings = XContentFactory.jsonBuilder();
        jsonSettings.startObject();
        jsonSettings.field("foo", tv2)
                .field("bar", 100)
                .field("baz", false);
        jsonSettings.endObject();
        configurationService.updateConfig(jsonSettings.bytes());

        assertThat(settingsListener.settings.getAsTime("foo", new TimeValue(0)).getMillis(), equalTo(tv2.getMillis()));
        assertThat(settingsListener.settings.getAsInt("bar", 0), equalTo(100));
        assertThat(settingsListener.settings.getAsBoolean("baz", true), equalTo(false));
    }

    public void testLoadingSettings() throws Exception {
        TimeValue tv = TimeValue.timeValueMillis(10000);
        Settings oldSettings = ImmutableSettings.builder()
                .put("foo", tv)
                .put("bar", 1)
                .put("baz", true)
                .build();

        TimeValue tv2 = TimeValue.timeValueMillis(10);
        Settings newSettings = ImmutableSettings.builder()
                .put("foo", tv2)
                .put("bar", 100)
                .put("baz", false)
                .build();

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject();
        newSettings.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        jsonBuilder.endObject();
        IndexResponse indexResponse = client()
                .prepareIndex(AlertsStore.ALERT_INDEX, ConfigurationService.CONFIG_TYPE, "global")
                .setSource(jsonBuilder)
                .get();
        assertTrue(indexResponse.isCreated());

        ConfigurationService configurationService = new ConfigurationService(oldSettings, client());
        Settings loadedSettings = configurationService.getConfig();
        assertThat(loadedSettings.get("foo"), equalTo(newSettings.get("foo")));
        assertThat(loadedSettings.get("bar"), equalTo(newSettings.get("bar")));
        assertThat(loadedSettings.get("baz"), equalTo(newSettings.get("baz")));
    }
}
