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
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.libs.arrow.Arrow;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.telemetry.TelemetryProvider;

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

    public ArrowBulkAction(Client client, TelemetryProvider telemetryProvider, Settings settings) {
        this.bulkHandler = new IncrementalBulkService(client, new IndexingPressure(settings), telemetryProvider.getMeterRegistry());
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
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        if (request.isStreamedContent() == false) {
            // FIXME: can we ever land here since supportsContentStream() returns true?
            BulkRequest bulkRequest = new BulkRequest();
            String defaultIndex = request.param("index");
            String defaultRouting = request.param("routing");
            FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
            String defaultPipeline = request.param("pipeline");
            boolean defaultListExecutedPipelines = request.paramAsBoolean("list_executed_pipelines", false);
            String waitForActiveShards = request.param("wait_for_active_shards");
            if (waitForActiveShards != null) {
                bulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
            }
            Boolean defaultRequireAlias = request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, false);
            boolean defaultRequireDataStream = request.paramAsBoolean(DocWriteRequest.REQUIRE_DATA_STREAM, false);
            bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
            bulkRequest.setRefreshPolicy(request.param("refresh"));
            bulkRequest.includeSourceOnError(RestUtils.getIncludeSourceOnError(request));
            bulkRequest.requestParamsUsed(request.params().keySet());
            ReleasableBytesReference content = request.content();
            String accept = request.header("Accept");
            boolean arrowResponse = accept == null || Arrow.MEDIA_TYPE.equals(accept);

            try {
                ArrowBulkRequestParser parser = new ArrowBulkRequestParser(request);
                parser.parse(
                    content,
                    defaultIndex,
                    defaultRouting,
                    defaultFetchSourceContext,
                    defaultPipeline,
                    defaultRequireAlias,
                    defaultRequireDataStream,
                    defaultListExecutedPipelines,
                    false,
                    null,
                    (req, type) -> bulkRequest.add(req),
                    bulkRequest::add,
                    bulkRequest::add
                );
            } catch (Exception e) {
                return channel -> new RestToXContentListener<>(channel).onFailure(
                    new ElasticsearchParseException("Failed to parse Arrow format", e)
                );
            }
            return channel -> {
                // FIXME: review ref counting and release in nominal and failure mode
                content.mustIncRef();
                var parent = new RestRefCountedChunkedToXContentListener<BulkResponse>(channel);
                client.bulk(bulkRequest, ActionListener.releaseAfter(new ArrowResponseListener(channel, arrowResponse, parent), content));
            };

        } else {

            String waitForActiveShards = request.param("wait_for_active_shards");
            TimeValue timeout = request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT);
            String refresh = request.param("refresh");
            // Return an Arrow response if Accept is missing or is the Arrow media type.
            // The Arrow response only contains failures, and will be an empty table if there are no failures.
            // Global (request-level) failures are always returned as JSON.
            String accept = request.header("Accept");
            boolean arrowResponse = accept == null || Arrow.MEDIA_TYPE.equals(accept);

            return new RestBulkAction.ChunkHandler(
                false,
                request,
                () -> bulkHandler.newBulkRequest(waitForActiveShards, timeout, refresh, request.params().keySet()),
                new ArrowBulkRequestParser(request)
            ) {
                @Override
                protected ActionListener<BulkResponse> createResponseListener(RestChannel channel) {
                    return new ArrowResponseListener(channel, arrowResponse, super.createResponseListener(channel));
                }
            };
        }
    }

    private record ArrowResponseListener(RestChannel channel, boolean arrowResponse, ActionListener<BulkResponse> parent)
        implements
            ActionListener<BulkResponse> {

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            if (arrowResponse == false) {
                // JSON response
                parent.onResponse(bulkItemResponses);
                return;
            }

            // FIXME: we can be more efficient and stream the response, like we do in ESQL's ArrowResponse
            var output = new BytesReferenceOutputStream();
            try (
                var allocator = Arrow.newChildAllocator("bulk_response", 0, 10_000_000L);
                var itemNoVector = new UInt4Vector(ERR_ITEM_NO, allocator);
                // Could be dictionary-encoded to reduce response size - only beneficial when there are errors
                var indexVector = new VarCharVector(INDEX, allocator);
                var idVector = new VarCharVector(ID, allocator);
                var statusVector = new UInt2Vector(ERR_STATUS, allocator);
                // Could be dictionary-encoded to reduce payload size
                var typeVector = new VarCharVector(ERR_TYPE, allocator);
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
