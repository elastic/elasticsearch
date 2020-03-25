/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
