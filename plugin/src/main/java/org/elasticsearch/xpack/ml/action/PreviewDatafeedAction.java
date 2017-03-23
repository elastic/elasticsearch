/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class PreviewDatafeedAction extends Action<PreviewDatafeedAction.Request, PreviewDatafeedAction.Response,
        PreviewDatafeedAction.RequestBuilder> {

    public static final PreviewDatafeedAction INSTANCE = new PreviewDatafeedAction();
    public static final String NAME = "cluster:admin/ml/datafeeds/preview";

    private PreviewDatafeedAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest implements ToXContent {

        private String datafeedId;

        Request() {
        }

        public Request(String datafeedId) {
            setDatafeedId(datafeedId);
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public final void setDatafeedId(String datafeedId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeedId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(datafeedId, other.datafeedId);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private BytesReference preview;

        Response() {
        }

        Response(BytesReference preview) {
            this.preview = preview;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            preview = in.readBytesReference();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBytesReference(preview);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.rawValue(preview, XContentType.JSON);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(preview);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(preview, other.preview);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final Client client;
        private final ClusterService clusterService;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Client client,
                               ClusterService clusterService) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.client = client;
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            MlMetadata mlMetadata = clusterService.state().getMetaData().custom(MlMetadata.TYPE);
            DatafeedConfig datafeed = mlMetadata.getDatafeed(request.getDatafeedId());
            if (datafeed == null) {
                throw ExceptionsHelper.missingDatafeedException(request.getDatafeedId());
            }
            Job job = mlMetadata.getJobs().get(datafeed.getJobId());
            if (job == null) {
                throw ExceptionsHelper.missingJobException(datafeed.getJobId());
            }
            DatafeedConfig.Builder datafeedWithAutoChunking = new DatafeedConfig.Builder(datafeed);
            datafeedWithAutoChunking.setChunkingConfig(ChunkingConfig.newAuto());
            // NB: this is using the client from the transport layer, NOT the internal client.
            // This is important because it means the datafeed search will fail if the user
            // requesting the preview doesn't have permission to search the relevant indices.
            DataExtractorFactory dataExtractorFactory = DataExtractorFactory.create(client, datafeedWithAutoChunking.build(), job);
            DataExtractor dataExtractor = dataExtractorFactory.newExtractor(0, System.currentTimeMillis());
            threadPool.generic().execute(() -> previewDatafeed(dataExtractor, listener));
        }

        /** Visible for testing */
        static void previewDatafeed(DataExtractor dataExtractor, ActionListener<Response> listener) {
            try {
                Optional<InputStream> inputStream = dataExtractor.next();
                // DataExtractor returns single-line JSON but without newline characters between objects.
                // Instead, it has a space between objects due to how JSON XContenetBuilder works.
                // In order to return a proper JSON array from preview, we surround with square brackets and
                // we stick in a comma between objects.
                // Also, the stream is expected to be a single line but in case it is not, we join lines
                // using space to ensure the comma insertion works correctly.
                StringBuilder responseBuilder = new StringBuilder("[");
                if (inputStream.isPresent()) {
                    try (BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream.get(), StandardCharsets.UTF_8))) {
                        responseBuilder.append(buffer.lines().collect(Collectors.joining(" ")).replace("} {", "},{"));
                    }
                }
                responseBuilder.append("]");
                listener.onResponse(new Response(new BytesArray(responseBuilder.toString().getBytes(StandardCharsets.UTF_8))));
            } catch (Exception e) {
                listener.onFailure(e);
            } finally {
                dataExtractor.cancel();
            }
        }
    }
}
