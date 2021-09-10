/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.settings.Settings;

public final class TestProfiles {

    private TestProfiles() {}

    /**
     * A pre-built light connection profile that shares a single connection across all
     * types.
     */
    public static final ConnectionProfile LIGHT_PROFILE;

    static {
        ConnectionProfile source = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.setConnectTimeout(source.getConnectTimeout());
        builder.setHandshakeTimeout(source.getHandshakeTimeout());
        builder.setCompressionEnabled(source.getCompressionEnabled());
        builder.setCompressionScheme(source.getCompressionScheme());
        builder.setPingInterval(source.getPingInterval());
        builder.addConnections(1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE);
        LIGHT_PROFILE = builder.build();
    }
}
