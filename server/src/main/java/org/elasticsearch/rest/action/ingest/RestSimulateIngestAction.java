/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.SimulateBulkAction;
import org.elasticsearch.action.bulk.SimulateBulkRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
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
import org.elasticsearch.rest.action.RestBuilderListener;
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

/**
 * This is the REST endpoint for the simulate ingest API. This API executes all pipelines for a document (or documents) that would be
 * executed if that document were sent to the given index. The JSON that would be indexed is returned to the user, along with the list of
 * pipelines that were executed. The API allows the user to optionally send in substitute definitions for pipelines so that changes can be
 * tried out without actually modifying the cluster state.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestSimulateIngestAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_ingest/_simulate"),
            new Route(POST, "/_ingest/_simulate"),
            new Route(GET, "/_ingest/{index}/_simulate"),
            new Route(POST, "/_ingest/{index}/_simulate")
        );
    }

    @Override
    public String getName() {
        return "ingest_simulate_ingest_action";
    }

    @Override
    @SuppressWarnings("unchecked")
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String defaultIndex = request.param("index");
        FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        String defaultPipeline = request.param("pipeline");
        Tuple<XContentType, ReleasableBytesReference> sourceTuple = request.contentOrSourceParam();
        Map<String, Object> sourceMap = XContentHelper.convertToMap(sourceTuple.v2(), false, sourceTuple.v1()).v2();
        Map<String, Map<String, Object>> pipelineSubstitutions = (Map<String, Map<String, Object>>) sourceMap.remove(
            "pipeline_substitutions"
        );
        Map<String, Map<String, Object>> componentTemplateSubstitutions = (Map<String, Map<String, Object>>) sourceMap.remove(
            "component_template_substitutions"
        );
        Map<String, Map<String, Object>> indexTemplateSubstitutions = (Map<String, Map<String, Object>>) sourceMap.remove(
            "index_template_substitutions"
        );
        Object mappingAddition = sourceMap.remove("mapping_addition");
        SimulateBulkRequest bulkRequest = new SimulateBulkRequest(
            pipelineSubstitutions == null ? Map.of() : pipelineSubstitutions,
            componentTemplateSubstitutions == null ? Map.of() : componentTemplateSubstitutions,
            indexTemplateSubstitutions == null ? Map.of() : indexTemplateSubstitutions,
            mappingAddition == null ? Map.of() : Map.of("_doc", mappingAddition)
        );
        BytesReference transformedData = convertToBulkRequestXContentBytes(sourceMap);
        bulkRequest.add(
            transformedData,
            defaultIndex,
            null,
            defaultFetchSourceContext,
            defaultPipeline,
            null,
            null,
            true,
            true,
            request.getXContentType(),
            request.getRestApiVersion()
        );
        return channel -> client.execute(SimulateBulkAction.INSTANCE, bulkRequest, new SimulateIngestRestToXContentListener(channel));
    }

    /*
     * The simulate ingest API is intended to have inputs and outputs that are formatted similarly to the simulate pipeline API for the
     * sake of consistency. But internally it uses the same code as the _bulk API, so that we have confidence that we are simulating what
     * really happens on ingest. This method transforms simulate-style inputs into an input that the bulk API can accept.
     * Non-private for unit testing
     */
    static BytesReference convertToBulkRequestXContentBytes(Map<String, Object> sourceMap) throws IOException {
        List<Map<String, Object>> docs = ConfigurationUtils.readList(null, null, sourceMap, "docs");
        if (docs.isEmpty()) {
            throw new IllegalArgumentException("must specify at least one document in [docs]");
        }
        ByteBuffer[] buffers = new ByteBuffer[2 * docs.size()];
        int bufferCount = 0;
        for (Map<String, Object> doc : docs) {
            if ((doc != null) == false) {
                throw new IllegalArgumentException("malformed [docs] section, should include an inner object");
            }
            Map<String, Object> document = ConfigurationUtils.readMap(null, null, doc, "_source");
            String index = ConfigurationUtils.readOptionalStringProperty(null, null, doc, IngestDocument.Metadata.INDEX.getFieldName());
            String id = ConfigurationUtils.readOptionalStringProperty(null, null, doc, IngestDocument.Metadata.ID.getFieldName());
            XContentBuilder actionXContentBuilder = XContentFactory.contentBuilder(XContentType.JSON).lfAtEnd();
            actionXContentBuilder.startObject().field("index").startObject();
            if (index != null) {
                actionXContentBuilder.field("_index", index);
            }
            if (id != null) {
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

    /*
     * The simulate ingest API is intended to have inputs and outputs that are formatted similarly to the simulate pipeline API for the
     * sake of consistency. But internally it uses the same code as the _bulk API, so that we have confidence that we are simulating what
     * really happens on ingest. This class is used in place of RestToXContentListener to transform simulate-style outputs into an
     * simulate-style xcontent.
     * Non-private for unit testing
     */
    static class SimulateIngestRestToXContentListener extends RestBuilderListener<BulkResponse> {

        SimulateIngestRestToXContentListener(RestChannel channel) {
            super(channel);
        }

        @Override
        public RestResponse buildResponse(BulkResponse response, XContentBuilder builder) throws Exception {
            assert response.isFragment() == false;
            toXContent(response, builder, channel.request());
            return new RestResponse(RestStatus.OK, builder);
        }

        private static void toXContent(BulkResponse response, XContentBuilder builder, ToXContent.Params params) throws IOException {
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
        }
    }
}
