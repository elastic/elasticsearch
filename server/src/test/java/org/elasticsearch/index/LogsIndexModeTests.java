/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperServiceTestCase;

public class LogsIndexModeTests extends MapperServiceTestCase {
    public void testConfigureIndex() {
        Settings s = getSettings();
        assertSame(IndexMode.LOGS, IndexSettings.MODE.get(s));
    }

    private Settings getSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), "logs")
            .build();
    }
}
