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

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestCountAction extends AbstractCatAction {

    @Inject
    public RestCountAction(Settings settings, RestController restController, RestController controller, Client client) {
        super(settings, controller, client);
        restController.registerHandler(GET, "/_cat/count", this);
        restController.registerHandler(GET, "/_cat/count/{index}", this);
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/count\n");
        sb.append("/_cat/count/{index}\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        CountRequest countRequest = new CountRequest(indices);
        String source = request.param("source");
        if (source != null) {
            countRequest.source(source);
        } else {
            QuerySourceBuilder querySourceBuilder = RestActions.parseQuerySource(request);
            if (querySourceBuilder != null) {
                countRequest.source(querySourceBuilder);
            }
        }

        client.count(countRequest, new RestResponseListener<CountResponse>(channel) {
            @Override
            public RestResponse buildResponse(CountResponse countResponse) throws Exception {
                return RestTable.buildResponse(buildTable(request, countResponse), channel);
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("epoch", "alias:t,time;desc:seconds since 1970-01-01 00:00:00, that the count was executed");
        table.addCell("timestamp", "alias:ts,hms;desc:time that the count was executed");
        table.addCell("count", "alias:dc,docs.count,docsCount;desc:the document count");
        table.endHeaders();
        return table;
    }

    private DateTimeFormatter dateFormat = DateTimeFormat.forPattern("HH:mm:ss");

    private Table buildTable(RestRequest request, CountResponse response) {
        Table table = getTableWithHeader(request);
        long time = System.currentTimeMillis();
        table.startRow();
        table.addCell(TimeUnit.SECONDS.convert(time, TimeUnit.MILLISECONDS));
        table.addCell(dateFormat.print(time));
        table.addCell(response.getCount());
        table.endRow();

        return table;
    }
}
