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

package org.elasticsearch.rest.action.main;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Classes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.jsr166y.ThreadLocalRandom;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RestMainAction extends BaseRestHandler {

    private final Map<String, Object> rootNode;

    private final int quotesSize;

    @Inject public RestMainAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        Map<String, Object> rootNode;
        int quotesSize;
        try {
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(Classes.getDefaultClassLoader().getResourceAsStream("org/elasticsearch/rest/action/main/quotes.json"));
            rootNode = parser.map();
            List arrayNode = (List) rootNode.get("quotes");
            quotesSize = arrayNode.size();
        } catch (Exception e) {
            rootNode = null;
            quotesSize = -1;
        }
        this.rootNode = rootNode;
        this.quotesSize = quotesSize;

        controller.registerHandler(GET, "/", this);
        controller.registerHandler(HEAD, "/", this);
    }

    @Override public void handleRequest(RestRequest request, RestChannel channel) {
        try {
            if (request.method() == RestRequest.Method.HEAD) {
                channel.sendResponse(new StringRestResponse(RestStatus.OK));
                return;
            }
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request).prettyPrint();
            builder.startObject();
            builder.field("ok", true);
            if (settings.get("name") != null) {
                builder.field("name", settings.get("name"));
            }
            builder.startObject("version").field("number", Version.number()).field("date", Version.date()).field("snapshot_build", Version.snapshotBuild()).endObject();
            builder.field("tagline", "You Know, for Search");
            builder.field("cover", "DON'T PANIC");
            if (rootNode != null) {
                builder.startObject("quote");
                List arrayNode = (List) rootNode.get("quotes");
                Map<String, Object> quoteNode = (Map<String, Object>) arrayNode.get(ThreadLocalRandom.current().nextInt(quotesSize));
                builder.field("book", quoteNode.get("book").toString());
                builder.field("chapter", quoteNode.get("chapter").toString());
                List textNodes = (List) quoteNode.get("text");
//                builder.startArray("text");
//                for (JsonNode textNode : textNodes) {
//                    builder.value(textNode.getValueAsText());
//                }
//                builder.endArray();
                int index = 0;
                for (Object textNode : textNodes) {
                    builder.field("text" + (++index), textNode.toString());
                }
                builder.endObject();
            }
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
        } catch (Exception e) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.warn("Failed to send response", e);
            }
        }
    }
}
