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

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestTable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestCountAction extends BaseRestHandler {

    private DateTimeFormatter dateFormat = DateTimeFormat.forPattern("HH:mm:ss");

    @Inject
    protected RestCountAction(Settings settings, Client client, RestController restController) {
        super(settings, client);
        restController.registerHandler(GET, "/_cat/count", this);
        restController.registerHandler(GET, "/_cat/count/{index}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        CountRequest countRequest = new CountRequest(indices);
        countRequest.operationThreading(BroadcastOperationThreading.SINGLE_THREAD);

        String source = request.param("source");
        if (source != null) {
            countRequest.query(source);
        } else {
            BytesReference querySource = RestActions.parseQuerySource(request);
            if (querySource != null) {
                countRequest.query(querySource, false);
            }
        }

        client.count(countRequest, new ActionListener<CountResponse>() {
            @Override
            public void onResponse(CountResponse countResponse) {
                try {
                    channel.sendResponse(RestTable.buildResponse(buildTable(countResponse), request, channel));
                } catch (Throwable t) {
                    onFailure(t);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, t));
                } catch (IOException e) {
                    logger.error("Failed to send failure response", e);
                }
            }
        });
    }

    private Table buildTable(CountResponse response) {

        Table table = new Table();
        table.startHeaders();
        table.addCell("time(ms)");
        table.addCell("timestamp");
        table.addCell("count");
        table.endHeaders();

        long time = System.currentTimeMillis();
        table.startRow();
        table.addCell(time);
        table.addCell(dateFormat.print(time));
        table.addCell(response.getCount());
        table.endRow();

        return table;
    }
}
