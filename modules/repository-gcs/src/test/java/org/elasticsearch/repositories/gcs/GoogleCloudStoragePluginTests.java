/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.util.List;

public class GoogleCloudStoragePluginTests extends ESTestCase {

    public void testExposedSettings() {
        List<Setting<?>> settings = new GoogleCloudStoragePlugin(Settings.EMPTY).getSettings();

        Assert.assertEquals(
            List.of(
                "gcs.client.*.credentials_file",
                "gcs.client.*.endpoint",
                "gcs.client.*.project_id",
                "gcs.client.*.connect_timeout",
                "gcs.client.*.read_timeout",
                "gcs.client.*.application_name",
                "gcs.client.*.token_uri",
                "gcs.client.*.proxy.type",
                "gcs.client.*.proxy.host",
                "gcs.client.*.proxy.port"
            ),
            settings.stream().map(Setting::getKey).toList()
        );
    }
}
