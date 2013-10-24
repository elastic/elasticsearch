/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
package org.elasticsearch.plugin.httprequest;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.rest.*;

public class TestHttpRequestRestAction extends BaseRestHandler {

    @Inject
    public TestHttpRequestRestAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_httprequestinfo", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) {
                     
        HttpRequest httpRequest = (HttpRequest) request; 
                       
        RestResponse response = new StringRestResponse(RestStatus.OK, "httprequestinfo");
        response.addHeader("localAddr", httpRequest.localAddr()+"");
        response.addHeader("remoteAddr", httpRequest.remoteAddr()+"");
        response.addHeader("opaqueId", httpRequest.opaqueId()+"");
        response.addHeader("localPort", httpRequest.localPort()+"");
        response.addHeader("remotePort", httpRequest.remotePort()+"");
        channel.sendResponse(response);
        
    }
}
