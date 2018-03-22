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

package org.elasticsearch.client;

import java.io.IOException;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;

/**
 * Light weight view into a {@link RestClient} that doesn't have any state of its own.
 */
class RestClientView extends AbstractRestClientActions {
    private final RestClient delegate;
    private final HostSelector hostSelector;

    protected RestClientView(RestClient delegate, HostSelector hostSelector) {
        this.delegate = delegate;
        this.hostSelector = hostSelector;
    }

    @Override
    final SyncResponseListener syncResponseListener() {
        return delegate.syncResponseListener();
    }

    @Override
    public final RestClientView withHostSelector(final HostSelector hostSelector) {
        final HostSelector inner = this.hostSelector;
        HostSelector combo = new HostSelector() {
            @Override
            public boolean select(HttpHost host, HostMetadata meta) {
                return inner.select(host, meta) && hostSelector.select(host, meta);
            }
        };
        return new RestClientView(delegate, combo);
    }

    @Override
    final void performRequestAsyncNoCatch(String method, String endpoint, Map<String, String> params,
            HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
            ResponseListener responseListener, Header[] headers) throws IOException {
        delegate.performRequestAsyncNoCatch(method, endpoint, params, entity, httpAsyncResponseConsumerFactory,
            responseListener, hostSelector, headers);
    }
}
