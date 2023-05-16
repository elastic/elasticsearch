/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.settings;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class PrivateSettingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(InternalOrPrivateSettingsPlugin.class);
    }

    public void testSetPrivateIndexSettingOnCreate() {
        final Settings settings = Settings.builder().put("index.private", "private").build();
        final Exception e = expectThrows(Exception.class, () -> createIndex("index", settings));
        assertThat(e, anyOf(instanceOf(IllegalArgumentException.class), instanceOf(ValidationException.class)));
        assertThat(e, hasToString(containsString("private index setting [index.private] can not be set explicitly")));
    }

    public void testUpdatePrivateIndexSettingViaSettingsAPI() {
        createIndex("test");
        // we can not update the setting via the update settings API
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.private", "private-update")).get()
        );
        final String message = "can not update private setting [index.private]; this setting is managed by Elasticsearch";
        assertThat(e, hasToString(containsString(message)));
        final GetSettingsResponse responseAfterAttemptedUpdate = client().admin().indices().prepareGetSettings("test").get();
        assertNull(responseAfterAttemptedUpdate.getSetting("test", "index.private"));
    }

    public void testUpdatePrivatelIndexSettingViaDedicatedAPI() {
        createIndex("test");
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request("test", "index.private", "private-update")
        ).actionGet();
        final GetSettingsResponse responseAfterUpdate = client().admin().indices().prepareGetSettings("test").get();
        assertThat(responseAfterUpdate.getSetting("test", "index.private"), equalTo("private-update"));
    }

}
