/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class ReindexPluginTests extends ESTestCase {

    public void testReindexPitKeepAliveSettingRegistered() throws IOException {
        ReindexPlugin plugin = new ReindexPlugin();
        try {
            List<Setting<?>> settings = plugin.getSettings();
            assertTrue(
                "getSettings() must include " + ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(),
                settings.stream().anyMatch(s -> s.getKey().equals(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey()))
            );
        } finally {
            plugin.close();
        }
    }
}
