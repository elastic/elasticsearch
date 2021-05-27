/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

public class BootstrapSettingsTests extends ESTestCase {

    public void testDefaultSettings() {
        assertTrue(BootstrapSettings.SECURITY_FILTER_BAD_DEFAULTS_SETTING.get(Settings.EMPTY));
        assertFalse(BootstrapSettings.MEMORY_LOCK_SETTING.get(Settings.EMPTY));
        assertTrue(BootstrapSettings.CTRLHANDLER_SETTING.get(Settings.EMPTY));
    }

}
