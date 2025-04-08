/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.metrics;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.NumericDocValuesField;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

public class MetricsTransportAction extends HandledTransportAction<
    MetricsTransportAction.MetricsRequest,
    MetricsTransportAction.MetricsResponse> {

    public static final String NAME = "indices:data/write/metrics";
    public static final ActionType<MetricsTransportAction.MetricsResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(MetricsTransportAction.class);

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndicesService indicesService;

    @Inject
    public MetricsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndicesService indicesService
    ) {
        super(NAME, transportService, actionFilters, MetricsRequest::new, threadPool.executor(ThreadPool.Names.WRITE));
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, MetricsRequest request, ActionListener<MetricsResponse> listener) {
        try {
            var metricsServiceRequest = ExportMetricsServiceRequest.parseFrom(request.exportMetricsServiceRequest.streamInput());

            logger.info("Received " + metricsServiceRequest.getResourceMetricsCount() + " metrics");

            final ClusterState clusterState = clusterService.state();
            final ProjectMetadata project = projectResolver.getProjectMetadata(clusterState);
            var indexAbstraction = project.getIndicesLookup().get(request.index);
            if (indexAbstraction == null) {
                throw new IllegalStateException("Index [" + request.index + "] does not exist");
            }
            var writeIndex = indexAbstraction.getWriteIndex();
            var indexService = indicesService.indexServiceSafe(writeIndex);

            // TODO proper routing ???
            var shard = indexService.getShard(0);
            var engine = shard.getEngineOrNull();

            // We receive a batch so there will be multiple documents as a result of processing it.
            var documents = createLuceneDocuments(metricsServiceRequest);

            // This loop is similar to TransportShardBulkAction, we fork the processing of the entire request
            // to ThreadPool.Names.WRITE but we perform all writes on the same thread.
            // We expect concurrency to come from a client submitting concurrent requests.
            for (var luceneDocument : documents) {
                var id = UUIDs.randomBase64UUID();

                var parsedDocument = new ParsedDocument(
                    // Even though this version field is here, it is not added to the LuceneDocument and won't be stored.
                    // This is just the contract that the code expects.
                    new NumericDocValuesField(NAME, -1L),
                    SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
                    id,
                    null,
                    List.of(luceneDocument),
                    null,
                    XContentType.JSON,
                    null,
                    0
                );
                var indexRequest = new Engine.Index(
                    Uid.encodeId(id),
                    parsedDocument,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    1,
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY,
                    System.nanoTime(),
                    System.currentTimeMillis(),
                    false,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    1
                );

                engine.index(indexRequest);
            }

            // TODO make sure to enable periodic flush on the index (?)

            listener.onResponse(new MetricsResponse());
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private List<LuceneDocument> createLuceneDocuments(ExportMetricsServiceRequest exportMetricsServiceRequest) {
        return List.of(new LuceneDocument());
    }

    public static class MetricsRequest extends ActionRequest {
        private String index;
        private BytesReference exportMetricsServiceRequest;

        public MetricsRequest(StreamInput in) throws IOException {
            super(in);
            index = in.readString();
            exportMetricsServiceRequest = in.readBytesReference();
        }

        public MetricsRequest(String index, BytesReference exportMetricsServiceRequest) {
            this.index = index;
            this.exportMetricsServiceRequest = exportMetricsServiceRequest;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class MetricsResponse extends ActionResponse {
        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
