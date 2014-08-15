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

package org.elasticsearch.client.support;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

/**
 * Client request headers picked up from the client settings. Applied to every
 * request sent by the client (both transport and node clients)
 */
public class Headers {

    public static final String PREFIX = "request.headers";

    public static final Headers EMPTY = new Headers(ImmutableSettings.EMPTY) {
        @Override
        public void applyTo(ActionRequest request) {
        }
    };

    private final Settings headers;

    @Inject
    public Headers(Settings settings) {
        headers = resolveHeaders(settings);
    }

    public void applyTo(ActionRequest request) {
        for (String key : headers.names()) {
            request.putHeader(key, headers.get(key));
        }
    }

    static Settings resolveHeaders(Settings settings) {
        Settings headers = settings.getAsSettings(PREFIX);
        return headers != null ? headers : ImmutableSettings.EMPTY;
    }

}
