/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsearch.transport.Netty4Plugin;

public final class MockNetty4Plugin extends Netty4Plugin {

    // see Netty4Plugin.... this runs without the permission from the netty4 module so it will fail since reindex can't set the property
    // to make it still work we disable that check for pseudo integ tests
    public MockNetty4Plugin(Settings settings) {
        super(Settings.builder().put(settings).put("netty.assert.buglevel", false).build());
    }

}
