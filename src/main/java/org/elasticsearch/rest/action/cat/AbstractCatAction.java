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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.io.UTF8StreamWriter;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.action.support.RestTable.buildHelpWidths;
import static org.elasticsearch.rest.action.support.RestTable.pad;

/**
 *
 */
public abstract class AbstractCatAction extends BaseRestHandler {

    public AbstractCatAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
    }

    abstract void doRequest(final RestRequest request, final RestChannel channel, final Client client);

    abstract void documentation(StringBuilder sb);

    abstract Table getTableWithHeader(final RestRequest request);

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        boolean helpWanted = request.paramAsBoolean("help", false);
        if (helpWanted) {
            Table table = getTableWithHeader(request);
            int[] width = buildHelpWidths(table, request);
            BytesStreamOutput bytesOutput = channel.bytesOutput();
            UTF8StreamWriter out = new UTF8StreamWriter().setOutput(bytesOutput);
            for (Table.Cell cell : table.getHeaders()) {
                // need to do left-align always, so create new cells
                pad(new Table.Cell(cell.value), width[0], request, out);
                out.append(" | ");
                pad(new Table.Cell(cell.attr.containsKey("alias") ? cell.attr.get("alias") : ""), width[1], request, out);
                out.append(" | ");
                pad(new Table.Cell(cell.attr.containsKey("desc") ? cell.attr.get("desc") : "not available"), width[2], request, out);
                out.append("\n");
            }
            out.close();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, bytesOutput.bytes()));
        } else {
            doRequest(request, channel, client);
        }
    }
}
