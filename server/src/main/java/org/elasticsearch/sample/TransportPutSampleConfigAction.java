/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;

import static org.elasticsearch.cluster.metadata.Metadata.ALL_CONTEXTS;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransportPutSampleConfigAction extends AcknowledgedTransportMasterNodeAction<PutSampleConfigAction.Request> {
    private static final Logger logger = LogManager.getLogger(TransportPutSampleConfigAction.class);
    private final ProjectResolver projectResolver;
    private final MasterServiceTaskQueue<UpdateSampleConfigTask> updateSamplingConfigTaskQueue;

    @Inject
    public TransportPutSampleConfigAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            PutSampleConfigAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutSampleConfigAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
        ClusterStateTaskExecutor<UpdateSampleConfigTask> updateMappingsExecutor = new SimpleBatchedAckListenerTaskExecutor<>() {

            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                UpdateSampleConfigTask updateSamplingConfigTask,
                ClusterState clusterState
            ) throws Exception {
                ProjectMetadata projectMetadata = clusterState.metadata().getProject(updateSamplingConfigTask.projectId);
                SamplingConfigCustomMetadata samplingConfig = projectMetadata.custom(SamplingConfigCustomMetadata.NAME);
                ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectMetadata);
                projectMetadataBuilder.putCustom(
                    SamplingConfigCustomMetadata.NAME,
                    new SamplingConfigCustomMetadata(
                        updateSamplingConfigTask.indexName,
                        updateSamplingConfigTask.rate,
                        updateSamplingConfigTask.maxSamples,
                        updateSamplingConfigTask.maxSize,
                        updateSamplingConfigTask.timeToLive,
                        updateSamplingConfigTask.condition
                    )
                );
                ClusterState updatedClusterState = ClusterState.builder(clusterState).putProjectMetadata(projectMetadataBuilder).build();
                return new Tuple<>(updatedClusterState, updateSamplingConfigTask);
            }
        };
        this.updateSamplingConfigTaskQueue = clusterService.createTaskQueue(
            "update-data-stream-mappings",
            Priority.NORMAL,
            updateMappingsExecutor
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        PutSampleConfigAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        ProjectId projectId = projectResolver.getProjectId();
        updateSamplingConfigTaskQueue.submitTask(
            "updating mappings on data stream",
            new UpdateSampleConfigTask(
                projectId,
                request.indices()[0],
                request.getRate(),
                request.getMaxSamples(),
                request.getMaxSize(),
                request.getTimeToLive(),
                request.getCondition(),
                request.ackTimeout(),
                listener
            ),
            request.masterNodeTimeout()
        );
        state.projectState(projectResolver.getProjectId()).metadata().custom("sample_config");
    }

    @Override
    protected ClusterBlockException checkBlock(PutSampleConfigAction.Request request, ClusterState state) {
        return null;
    }

    static class UpdateSampleConfigTask extends AckedBatchedClusterStateUpdateTask {
        final ProjectId projectId;
        private final String indexName;
        private final double rate;
        private final Integer maxSamples;
        private final ByteSizeValue maxSize;
        private final TimeValue timeToLive;
        private final String condition;

        UpdateSampleConfigTask(
            ProjectId projectId,
            String indexName,
            double rate,
            Integer maxSamples,
            ByteSizeValue maxSize,
            TimeValue timeToLive,
            String condition,
            TimeValue ackTimeout,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(ackTimeout, listener);
            this.projectId = projectId;
            this.indexName = indexName;
            this.rate = rate;
            this.maxSamples = maxSamples;
            this.maxSize = maxSize;
            this.timeToLive = timeToLive;
            this.condition = condition;
        }
    }

    public static final class SamplingConfigCustomMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom>
        implements
            Metadata.ProjectCustom {
        public final String indexName;
        public final double rate;
        public final Integer maxSamples;
        public final ByteSizeValue maxSize;
        public final TimeValue timeToLive;
        public final String condition;

        public static final String NAME = "sampling_config";
        public static final ParseField INDEX_NAME_FIELD = new ParseField("index_name");
        public static final ParseField RATE_FIELD = new ParseField("rate");
        public static final ParseField MAX_SAMPLES_FIELD = new ParseField("max_samples");
        public static final ParseField MAX_SIZE_FIELD = new ParseField("max_size");
        public static final ParseField TIME_TO_LIVE_FIELD = new ParseField("time_to_live");
        public static final ParseField CONDITION_FIELD = new ParseField("condition");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<SamplingConfigCustomMetadata, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            args -> new SamplingConfigCustomMetadata(
                (String) args[0],
                (double) args[1],
                (Integer) args[2],
                (Long) args[3],
                args[4] == null ? null : TimeValue.timeValueMillis((long) args[4]),
                (String) args[5]
            )
        );

        static {
            PARSER.declareString(constructorArg(), INDEX_NAME_FIELD);
            PARSER.declareDouble(constructorArg(), RATE_FIELD);
            PARSER.declareInt(optionalConstructorArg(), MAX_SAMPLES_FIELD);
            PARSER.declareLong(optionalConstructorArg(), MAX_SIZE_FIELD);
            PARSER.declareLong(optionalConstructorArg(), TIME_TO_LIVE_FIELD);
            PARSER.declareString(optionalConstructorArg(), CONDITION_FIELD);
        }

        public SamplingConfigCustomMetadata(
            String indexName,
            double rate,
            Integer maxSamples,
            ByteSizeValue maxSize,
            TimeValue timeToLive,
            String condition
        ) {
            this.indexName = indexName;
            this.rate = rate;
            this.maxSamples = maxSamples;
            this.maxSize = maxSize;
            this.timeToLive = timeToLive;
            this.condition = condition;
        }

        public SamplingConfigCustomMetadata(
            String indexName,
            double rate,
            Integer maxSamples,
            Long maxSizeInBytes,
            TimeValue timeToLive,
            String condition
        ) {
            this(
                indexName,
                rate,
                maxSamples,
                maxSizeInBytes == null ? null : ByteSizeValue.of(maxSizeInBytes, ByteSizeUnit.BYTES),
                timeToLive,
                condition
            );
        }

        public SamplingConfigCustomMetadata(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readDouble(),
                in.readOptionalInt(),
                in.readOptionalLong(),
                in.readOptionalTimeValue(),
                in.readOptionalString()
            );
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return ALL_CONTEXTS;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.RANDOM_SAMPLING;
        }

        @Override
        public String getWriteableName() {
            return "sample_config";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(indexName);
            out.writeDouble(rate);
            out.writeOptionalInt(maxSamples);
            out.writeOptionalLong(maxSize == null ? null : maxSize.getBytes());
            out.writeOptionalTimeValue(timeToLive);
            out.writeOptionalString(condition);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.single((b, p) -> {
                // b.startObject();
                b.field(INDEX_NAME_FIELD.getPreferredName(), indexName);
                b.field(RATE_FIELD.getPreferredName(), rate);
                if (maxSamples != null) {
                    b.field(MAX_SAMPLES_FIELD.getPreferredName(), maxSamples);
                }
                if (maxSize != null) {
                    b.field(MAX_SIZE_FIELD.getPreferredName(), maxSize.getBytes());
                }
                if (timeToLive != null) {
                    b.field(TIME_TO_LIVE_FIELD.getPreferredName(), timeToLive.millis());
                }
                if (condition != null) {
                    b.field(CONDITION_FIELD.getPreferredName(), condition);
                }
                // b.endObject();
                return b;
            });
        }

        public static SamplingConfigCustomMetadata fromXContent(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }
    }
}
