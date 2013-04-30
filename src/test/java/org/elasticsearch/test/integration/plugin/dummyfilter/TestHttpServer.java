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

package org.elasticsearch.test.integration.nodesinfo.plugin.dummyfilter;

import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.Base64;

import org.elasticsearch.rest.RestRequest;

import static org.elasticsearch.rest.RestStatus.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.StringRestResponse;

public class TestHttpServer extends HttpServer {

    public static String HEADER_NAME = "WWW-Authenticate";
    public static String HEADER_VALUE = "Basic realm='insert realm'";

    @Inject
    public TestHttpServer(Settings settings, Environment environment, HttpServerTransport transport,
            RestController restController, NodeService nodeService) {
        super(settings, environment, transport, restController, nodeService);
    }

    @Override
    public void internalDispatchRequest(HttpRequest request, HttpChannel channel) {
    	System.err.println("Adding the header!");
    	//channel.addHeader("foo", "bar");
		//super.internalDispatchRequest(request, channel);
		StringRestResponse resp = new StringRestResponse(UNAUTHORIZED, "Authentication Required");
		resp.addHeader(HEADER_NAME, HEADER_VALUE);
		channel.sendResponse(resp);
    }

}