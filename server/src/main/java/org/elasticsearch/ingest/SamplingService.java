/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.internal.XContentParserDecorator;
import org.elasticsearch.sample.TransportPutSampleConfigAction;
import org.elasticsearch.script.DynamicMap;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

import static org.elasticsearch.ingest.ConditionalProcessor.FUNCTIONS;

public class SamplingService {
    private static final Logger logger = LogManager.getLogger(SamplingService.class);
    private final ScriptService scriptService;
    private final LongSupplier relativeNanoTimeSupplier;
    private final MasterServiceTaskQueue<UpdateSampleConfigTask> updateSamplingConfigTaskQueue;
    private final Map<String, SampleInfo> samples = new HashMap<>();

    public SamplingService(ScriptService scriptService, ClusterService clusterService, LongSupplier relativeNanoTimeSupplier) {
        this.scriptService = scriptService;
        this.relativeNanoTimeSupplier = relativeNanoTimeSupplier;
        ClusterStateTaskExecutor<UpdateSampleConfigTask> updateSampleConfigExecutor = new SimpleBatchedAckListenerTaskExecutor<>() {

            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                UpdateSampleConfigTask updateSamplingConfigTask,
                ClusterState clusterState
            ) {
                ProjectMetadata projectMetadata = clusterState.metadata().getProject(updateSamplingConfigTask.projectId);
                TransportPutSampleConfigAction.SamplingConfigCustomMetadata samplingConfig = projectMetadata.custom(
                    TransportPutSampleConfigAction.SamplingConfigCustomMetadata.NAME
                );
                ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectMetadata);
                projectMetadataBuilder.putCustom(
                    TransportPutSampleConfigAction.SamplingConfigCustomMetadata.NAME,
                    new TransportPutSampleConfigAction.SamplingConfigCustomMetadata(
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
            updateSampleConfigExecutor
        );
    }

    public void updateSampleConfiguration(
        ProjectId projectId,
        String index,
        double rate,
        @Nullable Integer maxSamples,
        @Nullable ByteSizeValue maxSize,
        @Nullable TimeValue timeToLive,
        @Nullable String condition,
        @Nullable TimeValue masterNodeTimeout,
        @Nullable TimeValue ackTimeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        updateSamplingConfigTaskQueue.submitTask(
            "updating mappings on data stream",
            new UpdateSampleConfigTask(
                projectId,
                index,
                rate,
                maxSamples,
                maxSize,
                timeToLive,
                condition,
                ackTimeout,
                ActionListener.runBefore(listener, () -> samples.computeIfPresent(index, (s, sampleInfo) -> new SampleInfo()))
            ),
            masterNodeTimeout
        );
    }

    public void maybeSample(ProjectMetadata projectMetadata, IndexRequest indexRequest) throws IOException {
        maybeSample(
            projectMetadata,
            indexRequest,
            new IngestDocument(
                indexRequest.index(),
                indexRequest.id(),
                indexRequest.version(),
                indexRequest.routing(),
                indexRequest.versionType(),
                indexRequest.sourceAsMap(XContentParserDecorator.NOOP)
            )
        );
    }

    public void maybeSample(ProjectMetadata projectMetadata, IndexRequest indexRequest, IngestDocument ingestDocument) throws IOException {
        long startTime = relativeNanoTimeSupplier.getAsLong();
        TransportPutSampleConfigAction.SamplingConfigCustomMetadata samplingConfig = projectMetadata.custom(
            TransportPutSampleConfigAction.SamplingConfigCustomMetadata.NAME
        );
        if (samplingConfig != null) {
            String samplingIndex = samplingConfig.indexName;
            if (samplingIndex.equals(indexRequest.index())) {
                SampleInfo sampleInfo = samples.computeIfAbsent(samplingIndex, k -> new SampleInfo());
                SampleStats stats = sampleInfo.stats;
                stats.potentialSamples.increment();
                try {
                    if (sampleInfo.getSamples().size() < samplingConfig.maxSamples) {
                        String condition = samplingConfig.condition;
                        if (condition != null) {
                            if (sampleInfo.script == null || sampleInfo.factory == null) {
                                // We don't want to pay for synchronization because worst case, we compile the script twice
                                long compileScriptStartTime = relativeNanoTimeSupplier.getAsLong();
                                try {
                                    if (sampleInfo.compilationFailed) {
                                        // we don't want to waste time
                                        stats.samplesRejectedForException.increment();
                                        return;
                                    } else {
                                        Script script = getScript(condition);
                                        sampleInfo.setScript(script, scriptService.compile(script, IngestConditionalScript.CONTEXT));
                                    }
                                } catch (Exception e) {
                                    sampleInfo.compilationFailed = true;
                                    throw e;
                                } finally {
                                    stats.timeCompilingCondition.add((relativeNanoTimeSupplier.getAsLong() - compileScriptStartTime));
                                }
                            }
                        }
                        long conditionStartTime = relativeNanoTimeSupplier.getAsLong();
                        if (condition == null
                            || evaluateCondition(ingestDocument, sampleInfo.script, sampleInfo.factory, sampleInfo.stats)) {
                            stats.timeEvaluatingCondition.add((relativeNanoTimeSupplier.getAsLong() - conditionStartTime));
                            if (Math.random() < samplingConfig.rate) {
                                indexRequest.incRef();
                                if (indexRequest.source() instanceof ReleasableBytesReference releaseableSource) {
                                    releaseableSource.incRef();
                                }
                                sampleInfo.getSamples().add(indexRequest);
                                stats.samples.increment();
                                logger.info("Sampling " + indexRequest);
                            } else {
                                stats.samplesRejectedForRate.increment();
                            }
                        } else {
                            stats.samplesRejectedForCondition.increment();
                        }
                    } else {
                        stats.samplesRejectedForSize.increment();
                    }
                } catch (Exception e) {
                    stats.samplesRejectedForException.increment();
                    stats.lastException = e;
                    logger.info("Error performing sampling for " + samplingIndex, e);
                    // e.printStackTrace(System.out);
                } finally {
                    stats.timeSampling.add((relativeNanoTimeSupplier.getAsLong() - startTime));
                    logger.info("********* Stats: " + stats);
                }
            }
        }
    }

    public List<IndexRequest> getSamples(String index) {
        return samples.get(index).getSamples();
    }

    private boolean evaluateCondition(
        IngestDocument ingestDocument,
        Script script,
        IngestConditionalScript.Factory factory,
        SampleStats stats
    ) {
        return factory.newInstance(
            script.getParams(),
            new ConditionalProcessor.UnmodifiableIngestData(new DynamicMap(ingestDocument.getSourceAndMetadata(), FUNCTIONS))
        ).execute();
    }

    private static Script getScript(String conditional) throws IOException {
        logger.info("Parsing script for conditional " + conditional);
        try (
            XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).map(Map.of("source", conditional));
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                BytesReference.bytes(builder),
                XContentType.JSON
            )
        ) {
            return Script.parse(parser);
        }
    }

    private static final class SampleInfo {
        private final List<IndexRequest> samples;
        private final SampleStats stats;
        private volatile Script script;
        private volatile IngestConditionalScript.Factory factory;
        private volatile boolean compilationFailed = false;

        SampleInfo() {
            this.samples = new ArrayList<>();
            this.stats = new SampleStats();
        }

        public List<IndexRequest> getSamples() {
            return samples;
        }

        void setScript(Script script, IngestConditionalScript.Factory factory) {
            this.script = script;
            this.factory = factory;
        }
    }

    private static final class SampleStats {
        LongAdder potentialSamples = new LongAdder();
        LongAdder samplesRejectedForSize = new LongAdder();
        LongAdder samplesRejectedForCondition = new LongAdder();
        LongAdder samplesRejectedForRate = new LongAdder();
        LongAdder samplesRejectedForException = new LongAdder();
        LongAdder samples = new LongAdder();
        LongAdder timeSampling = new LongAdder();
        LongAdder timeEvaluatingCondition = new LongAdder();
        LongAdder timeCompilingCondition = new LongAdder();
        Exception lastException = null;

        @Override
        public String toString() {
            return "potentialSamples: "
                + potentialSamples
                + ", samplesRejectedForSize: "
                + samplesRejectedForSize
                + ", samplesRejectedForCondition: "
                + samplesRejectedForCondition
                + ", samplesRejectedForRate: "
                + samplesRejectedForRate
                + ", samplesRejectedForException: "
                + samplesRejectedForException
                + ", samples: "
                + samples
                + ", timeSampling: "
                + (timeSampling.longValue() / 1000000)
                + ", timeEvaluatingCondition: "
                + (timeEvaluatingCondition.longValue() / 1000000)
                + ", timeCompilingCondition: "
                + (timeCompilingCondition.longValue() / 1000000);
        }
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
}
