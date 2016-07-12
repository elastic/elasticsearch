/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.NettyPlugin;

public final class MockNettyPlugin extends NettyPlugin {
    // se NettyPlugin.... this runs without the permission from the netty module so it will fail since reindex can't set the property
    // to make it still work we disable that check for pseudo integ tests
    public MockNettyPlugin(Settings settings) {
        super(Settings.builder().put(settings).put("netty.assert.buglevel", false).build());
    }
}
