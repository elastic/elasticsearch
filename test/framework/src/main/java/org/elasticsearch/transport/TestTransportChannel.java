/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;

public class TestTransportChannel implements TransportChannel {

    private final ActionListener<TransportResponse> listener;

    public TestTransportChannel(ActionListener<TransportResponse> listener) {
        this.listener = listener;
    }

    @Override
    public String getProfileName() {
        return "default";
    }

    @Override
    public void sendResponse(TransportResponse response) {
        listener.onResponse(response);
    }

    @Override
    public void sendResponse(Exception exception) {
        listener.onFailure(exception);
    }

    @Override
    public String getChannelType() {
        return "test";
    }
}
