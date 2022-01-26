/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;

public class UpgradeIndexSettingsIT extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testSettingsVersion() {
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT.minimumIndexCompatibilityVersion())
                .build()
        );
        final long settingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        client().admin().indices().prepareUpgrade("test").get();
        assertThat(
            client().admin().cluster().prepareState().get().getState().metadata().index("test").getSettingsVersion(),
            equalTo(1 + settingsVersion)
        );
    }

}
