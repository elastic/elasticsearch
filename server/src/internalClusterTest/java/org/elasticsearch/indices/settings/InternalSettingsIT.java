/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.settings;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class InternalSettingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalOrPrivateSettingsPlugin.class);
    }

    public void testSetInternalIndexSettingOnCreate() {
        final Settings settings = Settings.builder().put("index.internal", "internal").build();
        createIndex("index", settings);
        final GetSettingsResponse response = client().admin().indices().prepareGetSettings("index").get();
        assertThat(response.getSetting("index", "index.internal"), equalTo("internal"));
    }

    public void testUpdateInternalIndexSettingViaSettingsAPI() {
        final Settings settings = Settings.builder().put("index.internal", "internal").build();
        createIndex("test", settings);
        final GetSettingsResponse response = client().admin().indices().prepareGetSettings("test").get();
        assertThat(response.getSetting("test", "index.internal"), equalTo("internal"));
        // we can not update the setting via the update settings API
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.internal", "internal-update"))
                .get()
        );
        final String message = "can not update internal setting [index.internal]; this setting is managed via a dedicated API";
        assertThat(e, hasToString(containsString(message)));
        final GetSettingsResponse responseAfterAttemptedUpdate = client().admin().indices().prepareGetSettings("test").get();
        assertThat(responseAfterAttemptedUpdate.getSetting("test", "index.internal"), equalTo("internal"));
    }

    public void testUpdateInternalIndexSettingViaDedicatedAPI() {
        final Settings settings = Settings.builder().put("index.internal", "internal").build();
        createIndex("test", settings);
        final GetSettingsResponse response = client().admin().indices().prepareGetSettings("test").get();
        assertThat(response.getSetting("test", "index.internal"), equalTo("internal"));
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request("test", "index.internal", "internal-update")
        ).actionGet();
        final GetSettingsResponse responseAfterUpdate = client().admin().indices().prepareGetSettings("test").get();
        assertThat(responseAfterUpdate.getSetting("test", "index.internal"), equalTo("internal-update"));
    }

}
