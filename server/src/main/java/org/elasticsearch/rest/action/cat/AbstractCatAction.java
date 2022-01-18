/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest.action.cat;

import org.elasticsearch.client.internal.node.NodeClient;
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
