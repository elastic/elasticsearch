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

import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

final class DelegatingTransportMessageListener implements TransportMessageListener {

    private final List<TransportMessageListener> listeners = new CopyOnWriteArrayList<>();

    @Override
    public void onRequestReceived(long requestId, String action) {
        for (TransportMessageListener listener : listeners) {
            listener.onRequestReceived(requestId, action);
        }
    }

    @Override
    public void onResponseSent(long requestId, String action, TransportResponse response) {
        for (TransportMessageListener listener : listeners) {
            listener.onResponseSent(requestId, action, response);
        }
    }

    @Override
    public void onResponseSent(long requestId, String action, Exception error) {
        for (TransportMessageListener listener : listeners) {
            listener.onResponseSent(requestId, action, error);
        }
    }

    @Override
    public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request,
                              TransportRequestOptions finalOptions) {
        for (TransportMessageListener listener : listeners) {
            listener.onRequestSent(node, requestId, action, request, finalOptions);
        }
    }

    @Override
    public void onResponseReceived(long requestId, Transport.ResponseContext holder) {
        for (TransportMessageListener listener : listeners) {
            listener.onResponseReceived(requestId, holder);
        }
    }

    public void addListener(TransportMessageListener listener) {
        listeners.add(listener);
    }

    public boolean removeListener(TransportMessageListener listener) {
        return listeners.remove(listener);
    }
}
