/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;

public class TransportServiceIT extends ESSingleNodeTestCase {
    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(TransportService.ENABLE_STACK_OVERFLOW_AVOIDANCE.getKey(), randomBoolean())
            .build();
    }

    public void testNodeStartsWithSetting() {
        // just check that the node starts with the setting set, i.e., that the setting is registered.
    }
}
