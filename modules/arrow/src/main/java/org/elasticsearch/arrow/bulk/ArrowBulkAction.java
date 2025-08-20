/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.libs.arrow.Arrow;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class ArrowBulkAction extends BaseRestHandler {

    public static final String ID = "_id";
    public static final String INDEX = "_index";
    public static final String ACTION = "_bulk_action";

    public static final String ERR_ITEM_NO = "item_no";
    public static final String ERR_STATUS = "status";
    public static final String ERR_TYPE = "type";
    public static final String ERR_REASON = "reason";

    private final IncrementalBulkService bulkHandler;

    public ArrowBulkAction(Client client, Settings settings) {
        this.bulkHandler = new IncrementalBulkService(client, new IndexingPressure(settings), MeterRegistry.NOOP);
    }

    @Override
    public String getName() {
        return "arrow_bulk_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_arrow/_bulk"),
            new Route(PUT, "/_arrow/_bulk"),
            new Route(POST, "/_arrow/{index}/_bulk"),
            new Route(PUT, "/_arrow/{index}/_bulk")
        );
    }

    public boolean mediaTypesValid(RestRequest request) {
        return ArrowBulkRequestParser.isArrowRequest(request);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String waitForActiveShards = request.param("wait_for_active_shards");
        TimeValue timeout = request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT);
        String refresh = request.param("refresh");
        return new RestBulkAction.ChunkHandler(
            false,
            request,
            () -> bulkHandler.newBulkRequest(waitForActiveShards, timeout, refresh),
            new ArrowBulkRequestParser(request)
        ) {
            @Override
            protected ActionListener<BulkResponse> createResponseListener(RestChannel channel) {
                // Return an Arrow response if Accept is missing or is the Arrow media type.
                // The Arrow response only contains failures, and will be an empty table if there are no failures.
                // Global (request-level) failures are always returned as JSON.

                var parent = super.createResponseListener(channel);
                var accept = request.header("Accept");
                if (accept == null || accept.equals(Arrow.MEDIA_TYPE)) {
                    return new ArrowResponseListener(channel, parent);
                } else {
                    return parent;
                }
            }
        };
    }

    private record ArrowResponseListener(RestChannel channel, ActionListener<BulkResponse> parent) implements ActionListener<BulkResponse> {

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            // FIXME: we can be more efficient and stream the response, like we do in ESQL's ArrowResponse
            var output = new BytesReferenceOutputStream();
            try (
                var allocator = Arrow.newChildAllocator("bulk_response", 0, 10_000_000L);
                var itemNoVector = new UInt4Vector(ERR_ITEM_NO, allocator);
                var indexVector = new VarCharVector(INDEX, allocator); // Could be dictionary-encoded
                var idVector = new VarCharVector(ID, allocator);
                var statusVector = new UInt2Vector(ERR_STATUS, allocator);
                var typeVector = new VarCharVector(ERR_TYPE, allocator); // Could be dictionary-encoded
                var reasonVector = new VarCharVector(ERR_REASON, allocator);

                var root = new VectorSchemaRoot(List.of(itemNoVector, indexVector, idVector, statusVector, typeVector, reasonVector));
                var writer = new ArrowStreamWriter(root, null, output);
            ) {
                int failureCount = 0;
                var items = bulkItemResponses.getItems();
                for (int itemNo = 0; itemNo < items.length; itemNo++) {
                    var item = items[itemNo];
                    if (item.isFailed()) {
                        BulkItemResponse.Failure failure = item.getFailure();

                        itemNoVector.setSafe(failureCount, itemNo);
                        addValue(indexVector, failureCount, failure.getIndex());
                        addValue(idVector, failureCount, failure.getId());
                        statusVector.setSafe(failureCount, failure.getStatus().getStatus());

                        Throwable cause = ExceptionsHelper.unwrapCause(failure.getCause());
                        addValue(typeVector, failureCount, cause == null ? null : ElasticsearchException.getExceptionName(cause));
                        addValue(reasonVector, failureCount, cause == null ? null : cause.getMessage());

                        failureCount++;
                    }
                }

                for (var vec : root.getFieldVectors()) {
                    vec.setValueCount(failureCount);
                }
                root.setRowCount(failureCount);

                writer.writeBatch();
            } catch (IOException e) {
                this.onFailure(e);
            }
            var response = new RestResponse(RestStatus.OK, Arrow.MEDIA_TYPE, output.asBytesReference());
            channel.sendResponse(response);
        }

        private void addValue(VarCharVector vector, int position, String value) {
            if (value == null) {
                vector.setNull(position);
            } else {
                vector.setSafe(position, value.getBytes(StandardCharsets.UTF_8));
            }
        }

        @Override
        public void onFailure(Exception e) {
            // Output the failure as JSON
            parent.onFailure(e);
        }
    }
}
