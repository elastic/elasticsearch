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

package org.elasticsearch.rest.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestResponse.Status.*;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.*;

/**
 * <pre>
 * { "index" : { "index" : "test", "type" : "type1", "id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * { "delete" : { "index" : "test", "type" : "type1", "id" : "2" } }
 * { "create" : { "index" : "test", "type" : "type1", "id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * </pre>
 *
 * @author kimchy (shay.banon)
 */
public class RestBulkAction extends BaseRestHandler {

    @Inject public RestBulkAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(POST, "/_bulk", this);
        controller.registerHandler(PUT, "/_bulk", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        BulkRequest bulkRequest = Requests.bulkRequest();

        int fromIndex = request.contentByteArrayOffset();
        byte[] data = request.contentByteArray();
        int length = request.contentLength();

        try {
            // first, guess the content
            XContent xContent = XContentFactory.xContent(data, fromIndex, length);
            byte marker = xContent.streamSeparator();
            while (true) {
                int nextMarker = findNextMarker(marker, fromIndex, data, length);
                if (nextMarker == -1) {
                    break;
                }
                // now parse the action
                XContentParser parser = xContent.createParser(data, fromIndex, nextMarker - fromIndex);

                // move pointers
                fromIndex = nextMarker + 1;

                // Move to START_OBJECT
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    continue;
                }
                assert token == XContentParser.Token.START_OBJECT;
                // Move to FIELD_NAME, that's the action
                token = parser.nextToken();
                assert token == XContentParser.Token.FIELD_NAME;
                String action = parser.currentName();
                // Move to START_OBJECT
                token = parser.nextToken();
                assert token == XContentParser.Token.START_OBJECT;

                String index = null;
                String type = null;
                String id = null;

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("index".equals(currentFieldName)) {
                            index = parser.text();
                        } else if ("type".equals(currentFieldName)) {
                            type = parser.text();
                        } else if ("id".equals(currentFieldName)) {
                            id = parser.text();
                        }
                    }
                }

                if ("delete".equals(action)) {
                    bulkRequest.add(new DeleteRequest(index, type, id));
                } else {
                    nextMarker = findNextMarker(marker, fromIndex, data, length);
                    if (nextMarker == -1) {
                        break;
                    }
                    bulkRequest.add(new IndexRequest(index, type, id)
                            .create("create".equals(action))
                            .source(data, fromIndex, nextMarker - fromIndex, request.contentUnsafe()));
                    // move pointers
                    fromIndex = nextMarker + 1;
                }
            }
        } catch (Exception e) {
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.bulk(bulkRequest, new ActionListener<BulkResponse>() {
            @Override public void onResponse(BulkResponse response) {
                try {
                    XContentBuilder builder = restContentBuilder(request);
                    builder.startObject();

                    builder.startArray("items");
                    for (BulkItemResponse itemResponse : response) {
                        builder.startObject(itemResponse.opType());
                        builder.field("index", itemResponse.index());
                        builder.field("type", itemResponse.type());
                        builder.field("id", itemResponse.id());
                        if (itemResponse.failed()) {
                            builder.field("error", itemResponse.failure().message());
                        }
                        builder.endObject();
                    }
                    builder.endArray();

                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
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

    private int findNextMarker(byte marker, int from, byte[] data, int length) {
        for (int i = from; i < length; i++) {
            if (data[i] == marker) {
                return i;
            }
        }
        return -1;
    }
}
