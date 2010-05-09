/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.client.transport.action.admin.cluster.ping.broadcast;

import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingRequest;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingResponse;
import org.elasticsearch.client.transport.action.support.BaseClientTransportAction;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class ClientTransportBroadcastPingAction extends BaseClientTransportAction<BroadcastPingRequest, BroadcastPingResponse> {

    @Inject public ClientTransportBroadcastPingAction(Settings settings, TransportService transportService) {
        super(settings, transportService, BroadcastPingResponse.class);
    }

    @Override protected String action() {
        return TransportActions.Admin.Cluster.Ping.BROADCAST;
    }
}