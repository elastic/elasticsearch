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

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.UTF8StreamWriter;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.rest.action.cat.RestTable.buildHelpWidths;
import static org.elasticsearch.rest.action.cat.RestTable.pad;

public abstract class AbstractCatAction extends BaseRestHandler {

    protected abstract RestChannelConsumer doCatRequest(RestRequest request, NodeClient client);

    protected abstract void documentation(StringBuilder sb);

    protected abstract Table getTableWithHeader(RestRequest request);

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        boolean helpWanted = request.paramAsBoolean("help", false);
        if (helpWanted) {
            return channel -> {
                Table table = getTableWithHeader(request);
                int[] width = buildHelpWidths(table, request);
                BytesStream bytesOutput = Streams.flushOnCloseStream(channel.bytesOutput());
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
            };
        } else {
            return doCatRequest(request, client);
        }
    }

    static Set<String> RESPONSE_PARAMS = Set.of("format", "h", "v", "ts", "pri", "bytes", "size", "time", "s");

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

}
