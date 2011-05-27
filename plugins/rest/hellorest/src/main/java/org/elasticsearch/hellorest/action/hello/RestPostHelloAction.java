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

package org.elasticsearch.hellorest.action.hello;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestStatus.*;

/**
 * @author itaifrenkel
 */
public class RestPostHelloAction extends BaseRestHandler {

    @Inject public RestPostHelloAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        logger.warn("settings" + this.getClass());
        controller.registerHandler(POST, "/{index}/{type}/_hello", this); // auto id creation;
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {

        // index a new document with automatic id and content '{ "hello" : "rest" }'
        IndexRequest indexRequest = new IndexRequest(request.param("index"), request.param("type"),null);
        indexRequest.source("hello","rest");
        client.index(indexRequest, new ActionListener<IndexResponse>() {
            @Override public void onResponse(IndexResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject()
                            .field(Fields.OK, true)
                            .field(Fields._INDEX, response.index())
                            .field(Fields._TYPE, response.type())
                            .field(Fields._ID, response.id());
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, CREATED, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    static final class Fields {
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
    }

}