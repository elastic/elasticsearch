/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.SimulateBulkAction;
import org.elasticsearch.action.bulk.SimulateBulkRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestSimulateIngestAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_ingest/_simulate"), new Route(POST, "/_ingest/_simulate"));
    }

    @Override
    public String getName() {
        return "ingest_simulate_ingest_action";
    }

    @Override
    @SuppressWarnings("unchecked")
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("type")) {
            request.param("type");
        }
        SimulateBulkRequest bulkRequest = new SimulateBulkRequest();
        String defaultIndex = request.param("index");
        String defaultRouting = null;
        FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        String defaultPipeline = request.param("pipeline");
        Boolean defaultRequireAlias = null;
        Tuple<XContentType, BytesReference> sourceTuple = request.contentOrSourceParam();
        Map<String, Object> sourceMap = XContentHelper.convertToMap(sourceTuple.v2(), false, sourceTuple.v1()).v2();
        bulkRequest.setPipelineSubstitutions((Map<String, Map<String, Object>>) sourceMap.remove("pipeline_substitutions"));
        BytesReference transformedData = convertToBulkRequestXContentBytes(sourceMap);
        bulkRequest.add(
            transformedData,
            defaultIndex,
            defaultRouting,
            defaultFetchSourceContext,
            defaultPipeline,
            defaultRequireAlias,
            true,
            true,
            request.getXContentType(),
            request.getRestApiVersion()
        );
        return channel -> client.execute(SimulateBulkAction.INSTANCE, bulkRequest, new SimulateIngestRestToXContentListener(channel));
    }

    private BytesReference convertToBulkRequestXContentBytes(Map<String, Object> sourceMap) throws IOException {
        List<Map<String, Object>> docs = ConfigurationUtils.readList(null, null, sourceMap, "docs");
        if (docs.isEmpty()) {
            throw new IllegalArgumentException("must specify at least one document in [docs]");
        }
        ByteBuffer[] buffers = new ByteBuffer[2 * docs.size()];
        int bufferCount = 0;
        for (Object object : docs) {
            if ((object instanceof Map) == false) {
                throw new IllegalArgumentException("malformed [docs] section, should include an inner object");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> dataMap = (Map<String, Object>) object;
            Map<String, Object> document = ConfigurationUtils.readMap(null, null, dataMap, "_source");
            String index = ConfigurationUtils.readStringOrIntProperty(
                null,
                null,
                dataMap,
                IngestDocument.Metadata.INDEX.getFieldName(),
                "_index"
            );
            String id = ConfigurationUtils.readStringOrIntProperty(null, null, dataMap, IngestDocument.Metadata.ID.getFieldName(), "_id");
            String routing = ConfigurationUtils.readOptionalStringOrIntProperty(
                null,
                null,
                dataMap,
                IngestDocument.Metadata.ROUTING.getFieldName()
            );
            XContentBuilder actionXContentBuilder = XContentFactory.contentBuilder(XContentType.JSON).lfAtEnd();
            actionXContentBuilder.startObject().field("index").startObject();
            if ("_index".equals(index) == false) {
                actionXContentBuilder.field("_index", index);
            }
            if ("id".equals(id) == false) {
                actionXContentBuilder.field("_id", id);
            }
            actionXContentBuilder.endObject().endObject();
            buffers[bufferCount++] = ByteBuffer.wrap(BytesReference.bytes(actionXContentBuilder).toBytesRef().bytes);
            XContentBuilder dataXContentBuilder = XContentFactory.contentBuilder(XContentType.JSON).lfAtEnd();
            dataXContentBuilder.startObject();
            for (String key : document.keySet()) {
                dataXContentBuilder.field(key, document.get(key));
            }
            dataXContentBuilder.endObject();
            buffers[bufferCount++] = ByteBuffer.wrap(BytesReference.bytes(dataXContentBuilder).toBytesRef().bytes);
        }
        return BytesReference.fromByteBuffers(buffers);
    }

    private class SimulateIngestRestToXContentListener extends RestToXContentListener<BulkResponse> {

        SimulateIngestRestToXContentListener(RestChannel channel) {
            super(channel);
        }

        public RestResponse buildResponse(BulkResponse response, XContentBuilder builder) throws Exception {
            assert response.isFragment() == false; // would be nice if we could make default methods final
            toXContent(response, builder, channel.request());
            RestStatus restStatus = statusFunction.apply(response);
            return new RestResponse(restStatus, builder);
        }

        private static XContentBuilder toXContent(BulkResponse response, XContentBuilder builder, ToXContent.Params params)
            throws IOException {
            builder.startObject();
            builder.startArray("docs");
            for (BulkItemResponse item : response) {
                builder.startObject();
                builder.startObject("doc");
                if (item.isFailed()) {
                    builder.field("_id", item.getFailure().getId());
                    builder.field("_index", item.getFailure().getIndex());
                    builder.startObject("error");
                    ElasticsearchException.generateThrowableXContent(builder, params, item.getFailure().getCause());
                    builder.endObject();
                } else {
                    item.getResponse().innerToXContent(builder, params);
                }
                builder.endObject();
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }
}
