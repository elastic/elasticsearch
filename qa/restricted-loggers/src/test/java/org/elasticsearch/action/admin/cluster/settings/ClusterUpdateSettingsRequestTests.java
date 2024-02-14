/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.logging.Level;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class ClusterUpdateSettingsRequestTests extends ESTestCase {

    public void testSettingRestrictedLoggers() {
        assertThat(Loggers.RESTRICTED_LOGGERS, hasSize(greaterThan(0)));

        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        for (String logger : Loggers.RESTRICTED_LOGGERS) {
            var validation = request.persistentSettings(Map.of("logger." + logger, Level.DEBUG)).validate();
            assertNotNull(validation);
            assertThat(validation.validationErrors(), contains("Level [DEBUG] is not permitted for logger [" + logger + "]"));
            // INFO is permitted
            assertNull(request.persistentSettings(Map.of("logger." + logger, Level.INFO)).validate());
        }
    }
}
