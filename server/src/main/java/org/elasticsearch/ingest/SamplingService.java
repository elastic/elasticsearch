/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.admin.indices.sampling.SamplingConfiguration;
import org.elasticsearch.action.admin.indices.sampling.SamplingMetadata;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class SamplingService implements ClusterStateListener {
    public static final boolean RANDOM_SAMPLING_FEATURE_FLAG = new FeatureFlag("random_sampling").isEnabled();
    private static final Logger logger = LogManager.getLogger(SamplingService.class);
    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final LongSupplier relativeMillisTimeSupplier;
    private final LongSupplier statsTimeSupplier = System::nanoTime;
    private final Random random;
    /*
     * This Map contains the samples that exist on this node. They are not persisted to disk. They are stored as SoftReferences so that
     * sampling does not contribute to a node running out of memory. The idea is that access to samples is desirable, but not critical. We
     * make a best effort to keep them around, but do not worry about the complexity or cost of making them durable.
     */
    private final Map<ProjectIndex, SoftReference<SampleInfo>> samples = new ConcurrentHashMap<>();

    public SamplingService(
        ScriptService scriptService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        LongSupplier relativeMillisTimeSupplier
    ) {
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.relativeMillisTimeSupplier = relativeMillisTimeSupplier;
        random = Randomness.get();
    }

    /**
     * Potentially samples the given indexRequest, depending on the existing sampling configuration.
     * @param projectMetadata Used to get the sampling configuration
     * @param indexRequest The raw request to potentially sample
     */
    public void maybeSample(ProjectMetadata projectMetadata, IndexRequest indexRequest) {
        maybeSample(projectMetadata, indexRequest.index(), indexRequest, () -> {
            /*
             * The conditional scripts used by random sampling work off of IngestDocuments, in the same way conditionals do in pipelines. In
             * this case, we did not have an IngestDocument (which happens when there are no pipelines). So we construct one with the same
             * fields as this IndexRequest for use in conditionals. It is created in this lambda to avoid the expensive sourceAsMap call
             * if the condition is never executed.
             */
            Map<String, Object> sourceAsMap;
            try {
                sourceAsMap = indexRequest.sourceAsMap();
            } catch (XContentParseException e) {
                sourceAsMap = Map.of();
                logger.trace("Invalid index request source, attempting to sample anyway");
            }
            return new IngestDocument(
                indexRequest.index(),
                indexRequest.id(),
                indexRequest.version(),
                indexRequest.routing(),
                indexRequest.versionType(),
                sourceAsMap
            );
        });
    }

    /**
     * Potentially samples the given indexRequest, depending on the existing sampling configuration.
     * @param projectMetadata Used to get the sampling configuration
     * @param indexRequest The raw request to potentially sample
     * @param ingestDocument The IngestDocument used for evaluating any conditionals that are part of the sample configuration
     */
    public void maybeSample(ProjectMetadata projectMetadata, String indexName, IndexRequest indexRequest, IngestDocument ingestDocument) {
        maybeSample(projectMetadata, indexName, indexRequest, () -> ingestDocument);
    }

    private void maybeSample(
        ProjectMetadata projectMetadata,
        String indexName,
        IndexRequest indexRequest,
        Supplier<IngestDocument> ingestDocumentSupplier
    ) {
        if (RANDOM_SAMPLING_FEATURE_FLAG == false) {
            return;
        }
        long startTime = statsTimeSupplier.getAsLong();
        SamplingMetadata samplingMetadata = projectMetadata.custom(SamplingMetadata.TYPE);
        if (samplingMetadata == null) {
            return;
        }
        SamplingConfiguration samplingConfig = samplingMetadata.getIndexToSamplingConfigMap().get(indexName);
        ProjectId projectId = projectMetadata.id();
        if (samplingConfig == null) {
            return;
        }
        SoftReference<SampleInfo> sampleInfoReference = samples.compute(
            new ProjectIndex(projectId, indexName),
            (k, v) -> v == null || v.get() == null
                ? new SoftReference<>(
                    new SampleInfo(samplingConfig.maxSamples(), samplingConfig.timeToLive(), relativeMillisTimeSupplier.getAsLong())
                )
                : v
        );
        SampleInfo sampleInfo = sampleInfoReference.get();
        if (sampleInfo == null) {
            return;
        }
        SampleStats stats = sampleInfo.stats;
        stats.potentialSamples.increment();
        try {
            if (sampleInfo.hasCapacity() == false) {
                stats.samplesRejectedForMaxSamplesExceeded.increment();
                return;
            }
            if (random.nextDouble() >= samplingConfig.rate()) {
                stats.samplesRejectedForRate.increment();
                return;
            }
            String condition = samplingConfig.condition();
            if (condition != null) {
                if (sampleInfo.script == null || sampleInfo.factory == null) {
                    // We don't want to pay for synchronization because worst case, we compile the script twice
                    long compileScriptStartTime = statsTimeSupplier.getAsLong();
                    try {
                        if (sampleInfo.compilationFailed) {
                            // we don't want to waste time -- if the script failed to compile once it will just fail again
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
                        stats.timeCompilingCondition.add((statsTimeSupplier.getAsLong() - compileScriptStartTime));
                    }
                }
            }
            if (condition != null
                && evaluateCondition(ingestDocumentSupplier, sampleInfo.script, sampleInfo.factory, sampleInfo.stats) == false) {
                stats.samplesRejectedForCondition.increment();
                return;
            }
            RawDocument sample = getRawDocumentForIndexRequest(projectId, indexName, indexRequest);
            if (sampleInfo.offer(sample)) {
                stats.samples.increment();
                logger.trace("Sampling " + indexRequest);
            } else {
                stats.samplesRejectedForMaxSamplesExceeded.increment();
            }
        } catch (Exception e) {
            stats.samplesRejectedForException.increment();
            /*
             * We potentially overwrite a previous exception here. But the thinking is that the user will pretty rapidly iterate on
             * exceptions as they come up, and this avoids the overhead and complexity of keeping track of multiple exceptions.
             */
            stats.lastException = e;
            logger.debug("Error performing sampling for " + indexName, e);
        } finally {
            stats.timeSampling.add((statsTimeSupplier.getAsLong() - startTime));
        }
    }

    /**
     * Gets the sample for the given projectId and index on this node only. The sample is not persistent.
     * @param projectId The project that this sample is for
     * @param index The index that the sample is for
     * @return The raw documents in the sample on this node, or an empty list if there are none
     */
    public List<RawDocument> getLocalSample(ProjectId projectId, String index) {
        SoftReference<SampleInfo> sampleInfoReference = samples.get(new ProjectIndex(projectId, index));
        SampleInfo sampleInfo = sampleInfoReference == null ? null : sampleInfoReference.get();
        return sampleInfo == null ? List.of() : Arrays.stream(sampleInfo.getRawDocuments()).filter(Objects::nonNull).toList();
    }

    /**
     * Gets the sample stats for the given projectId and index on this node only. The stats are not persistent. They are reset when the
     * node restarts for example.
     * @param projectId The project that this sample is for
     * @param index The index that the sample is for
     * @return Current stats on this node for this sample
     */
    public SampleStats getLocalSampleStats(ProjectId projectId, String index) {
        SoftReference<SampleInfo> sampleInfoReference = samples.get(new ProjectIndex(projectId, index));
        SampleInfo sampleInfo = sampleInfoReference.get();
        return sampleInfo == null ? new SampleStats() : sampleInfo.stats;
    }

    public boolean atLeastOneSampleConfigured() {
        if (RANDOM_SAMPLING_FEATURE_FLAG) {
            SamplingMetadata samplingMetadata = clusterService.state()
                .projectState(projectResolver.getProjectId())
                .metadata()
                .custom(SamplingMetadata.TYPE);
            return samplingMetadata != null && samplingMetadata.getIndexToSamplingConfigMap().isEmpty() == false;
        } else {
            return false;
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO: React to sampling config changes
    }

    private boolean evaluateCondition(
        Supplier<IngestDocument> ingestDocumentSupplier,
        Script script,
        IngestConditionalScript.Factory factory,
        SampleStats stats
    ) {
        long conditionStartTime = statsTimeSupplier.getAsLong();
        boolean passedCondition = factory.newInstance(script.getParams(), ingestDocumentSupplier.get().getUnmodifiableSourceAndMetadata())
            .execute();
        stats.timeEvaluatingCondition.add((statsTimeSupplier.getAsLong() - conditionStartTime));
        return passedCondition;
    }

    private static Script getScript(String conditional) throws IOException {
        logger.debug("Parsing script for conditional " + conditional);
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

    /*
     * This represents a raw document as the user sent it to us in an IndexRequest. It only holds onto the information needed for the
     * sampling API, rather than holding all of the fields a user might send in an IndexRequest.
     */
    public record RawDocument(ProjectId projectId, String indexName, byte[] source, XContentType contentType) implements Writeable {

        public RawDocument(StreamInput in) throws IOException {
            this(ProjectId.readFrom(in), in.readString(), in.readByteArray(), in.readEnum(XContentType.class));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            projectId.writeTo(out);
            out.writeString(indexName);
            out.writeByteArray(source);
            XContentHelper.writeTo(out, contentType);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RawDocument rawDocument = (RawDocument) o;
            return Objects.equals(projectId, rawDocument.projectId)
                && Objects.equals(indexName, rawDocument.indexName)
                && Arrays.equals(source, rawDocument.source)
                && contentType == rawDocument.contentType;
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(projectId, indexName, contentType);
            result = 31 * result + Arrays.hashCode(source);
            return result;
        }
    }

    /*
     * This creates a RawDocument from the indexRequest. The source bytes of the indexRequest are copied into the RawDocument. So the
     * RawDocument might be a relatively expensive object memory-wise. Since the bytes are copied, subsequent changes to the indexRequest
     * are not reflected in the RawDocument
     */
    private RawDocument getRawDocumentForIndexRequest(ProjectId projectId, String indexName, IndexRequest indexRequest) {
        BytesReference sourceReference = indexRequest.source();
        assert sourceReference != null : "Cannot sample an IndexRequest with no source";
        byte[] source = sourceReference.array();
        final byte[] sourceCopy = new byte[sourceReference.length()];
        System.arraycopy(source, sourceReference.arrayOffset(), sourceCopy, 0, sourceReference.length());
        return new RawDocument(projectId, indexName, sourceCopy, indexRequest.getContentType());
    }

    public static final class SampleStats implements Writeable, ToXContent {
        // These are all non-private for the sake of unit testing
        final LongAdder samples = new LongAdder();
        final LongAdder potentialSamples = new LongAdder();
        final LongAdder samplesRejectedForMaxSamplesExceeded = new LongAdder();
        final LongAdder samplesRejectedForCondition = new LongAdder();
        final LongAdder samplesRejectedForRate = new LongAdder();
        final LongAdder samplesRejectedForException = new LongAdder();
        final LongAdder timeSampling = new LongAdder();
        final LongAdder timeEvaluatingCondition = new LongAdder();
        final LongAdder timeCompilingCondition = new LongAdder();
        Exception lastException = null;

        public SampleStats() {}

        public SampleStats(StreamInput in) throws IOException {
            potentialSamples.add(in.readLong());
            samplesRejectedForMaxSamplesExceeded.add(in.readLong());
            samplesRejectedForCondition.add(in.readLong());
            samplesRejectedForRate.add(in.readLong());
            samplesRejectedForException.add(in.readLong());
            samples.add(in.readLong());
            timeSampling.add(in.readLong());
            timeEvaluatingCondition.add(in.readLong());
            timeCompilingCondition.add(in.readLong());
            if (in.readBoolean()) {
                lastException = in.readException();
            } else {
                lastException = null;
            }
        }

        public long getSamples() {
            return samples.longValue();
        }

        public long getPotentialSamples() {
            return potentialSamples.longValue();
        }

        public long getSamplesRejectedForMaxSamplesExceeded() {
            return samplesRejectedForMaxSamplesExceeded.longValue();
        }

        public long getSamplesRejectedForCondition() {
            return samplesRejectedForCondition.longValue();
        }

        public long getSamplesRejectedForRate() {
            return samplesRejectedForRate.longValue();
        }

        public long getSamplesRejectedForException() {
            return samplesRejectedForException.longValue();
        }

        public TimeValue getTimeSampling() {
            return TimeValue.timeValueNanos(timeSampling.longValue());
        }

        public TimeValue getTimeEvaluatingCondition() {
            return TimeValue.timeValueNanos(timeEvaluatingCondition.longValue());
        }

        public TimeValue getTimeCompilingCondition() {
            return TimeValue.timeValueNanos(timeCompilingCondition.longValue());
        }

        public Exception getLastException() {
            return lastException;
        }

        @Override
        public String toString() {
            return "potential_samples: "
                + potentialSamples
                + ", samples_rejected_for_max_samples_exceeded: "
                + samplesRejectedForMaxSamplesExceeded
                + ", samples_rejected_for_condition: "
                + samplesRejectedForCondition
                + ", samples_rejected_for_rate: "
                + samplesRejectedForRate
                + ", samples_rejected_for_exception: "
                + samplesRejectedForException
                + ", samples_accepted: "
                + samples
                + ", time_sampling: "
                + (timeSampling.longValue() / 1000000)
                + ", time_evaluating_condition: "
                + (timeEvaluatingCondition.longValue() / 1000000)
                + ", time_compiling_condition: "
                + (timeCompilingCondition.longValue() / 1000000);
        }

        public SampleStats combine(SampleStats other) {
            SampleStats result = new SampleStats();
            addAllFields(this, result);
            addAllFields(other, result);
            return result;
        }

        private static void addAllFields(SampleStats source, SampleStats dest) {
            dest.potentialSamples.add(source.potentialSamples.longValue());
            dest.samplesRejectedForMaxSamplesExceeded.add(source.samplesRejectedForMaxSamplesExceeded.longValue());
            dest.samplesRejectedForCondition.add(source.samplesRejectedForCondition.longValue());
            dest.samplesRejectedForRate.add(source.samplesRejectedForRate.longValue());
            dest.samplesRejectedForException.add(source.samplesRejectedForException.longValue());
            dest.samples.add(source.samples.longValue());
            dest.timeSampling.add(source.timeSampling.longValue());
            dest.timeEvaluatingCondition.add(source.timeEvaluatingCondition.longValue());
            dest.timeCompilingCondition.add(source.timeCompilingCondition.longValue());
            if (dest.lastException == null) {
                dest.lastException = source.lastException;
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("potential_samples", potentialSamples.longValue());
            builder.field("samples_rejected_for_max_samples_exceeded", samplesRejectedForMaxSamplesExceeded.longValue());
            builder.field("samples_rejected_for_condition", samplesRejectedForCondition.longValue());
            builder.field("samples_rejected_for_rate", samplesRejectedForRate.longValue());
            builder.field("samples_rejected_for_exception", samplesRejectedForException.longValue());
            builder.field("samples_accepted", samples.longValue());
            builder.field("time_sampling", (timeSampling.longValue() / 1000000));
            builder.field("time_evaluating_condition", (timeEvaluatingCondition.longValue() / 1000000));
            builder.field("time_compiling_condition", (timeCompilingCondition.longValue() / 1000000));
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(potentialSamples.longValue());
            out.writeLong(samplesRejectedForMaxSamplesExceeded.longValue());
            out.writeLong(samplesRejectedForCondition.longValue());
            out.writeLong(samplesRejectedForRate.longValue());
            out.writeLong(samplesRejectedForException.longValue());
            out.writeLong(samples.longValue());
            out.writeLong(timeSampling.longValue());
            out.writeLong(timeEvaluatingCondition.longValue());
            out.writeLong(timeCompilingCondition.longValue());
            if (lastException == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeException(lastException);
            }
        }

        /*
         * equals and hashCode are implemented for the sake of testing serialization. Since this class is mutable, these ought to never be
         * used outside of testing.
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SampleStats that = (SampleStats) o;
            if (samples.longValue() != that.samples.longValue()) {
                return false;
            }
            if (potentialSamples.longValue() != that.potentialSamples.longValue()) {
                return false;
            }
            if (samplesRejectedForMaxSamplesExceeded.longValue() != that.samplesRejectedForMaxSamplesExceeded.longValue()) {
                return false;
            }
            if (samplesRejectedForCondition.longValue() != that.samplesRejectedForCondition.longValue()) {
                return false;
            }
            if (samplesRejectedForRate.longValue() != that.samplesRejectedForRate.longValue()) {
                return false;
            }
            if (samplesRejectedForException.longValue() != that.samplesRejectedForException.longValue()) {
                return false;
            }
            if (timeSampling.longValue() != that.timeSampling.longValue()) {
                return false;
            }
            if (timeEvaluatingCondition.longValue() != that.timeEvaluatingCondition.longValue()) {
                return false;
            }
            if (timeCompilingCondition.longValue() != that.timeCompilingCondition.longValue()) {
                return false;
            }
            return exceptionsAreEqual(lastException, that.lastException);
        }

        private boolean exceptionsAreEqual(Exception e1, Exception e2) {
            if (e1 == null && e2 == null) {
                return true;
            }
            if (e1 == null || e2 == null) {
                return false;
            }
            return e1.getClass().equals(e2.getClass()) && e1.getMessage().equals(e2.getMessage());
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                samples.longValue(),
                potentialSamples.longValue(),
                samplesRejectedForMaxSamplesExceeded.longValue(),
                samplesRejectedForCondition.longValue(),
                samplesRejectedForRate.longValue(),
                samplesRejectedForException.longValue(),
                timeSampling.longValue(),
                timeEvaluatingCondition.longValue(),
                timeCompilingCondition.longValue()
            ) + hashException(lastException);
        }

        private int hashException(Exception e) {
            if (e == null) {
                return 0;
            } else {
                return Objects.hash(e.getClass(), e.getMessage());
            }
        }
    }

    /*
     * This is used internally to store information about a sample in the samples Map.
     */
    private static final class SampleInfo {
        private final RawDocument[] rawDocuments;
        private final SampleStats stats;
        private final long expiration;
        private final TimeValue timeToLive;
        private volatile Script script;
        private volatile IngestConditionalScript.Factory factory;
        private volatile boolean compilationFailed = false;
        private volatile boolean isFull = false;
        private final AtomicInteger arrayIndex = new AtomicInteger(0);

        SampleInfo(int maxSamples, TimeValue timeToLive, long relativeNowMillis) {
            this.timeToLive = timeToLive;
            this.rawDocuments = new RawDocument[maxSamples];
            this.stats = new SampleStats();
            this.expiration = (timeToLive == null ? TimeValue.timeValueDays(5).millis() : timeToLive.millis()) + relativeNowMillis;
        }

        public boolean hasCapacity() {
            return isFull == false;
        }

        /*
         * This returns the array of raw documents. It's size will be the maximum number of raw documents allowed in this sample. Some (or
         * all) elements could be null.
         */
        public RawDocument[] getRawDocuments() {
            return rawDocuments;
        }

        /*
         * Adds the rawDocument to the sample if there is capacity. Returns true if it adds it, or false if it does not.
         */
        public boolean offer(RawDocument rawDocument) {
            int index = arrayIndex.getAndIncrement();
            if (index < rawDocuments.length) {
                rawDocuments[index] = rawDocument;
                if (index == rawDocuments.length - 1) {
                    isFull = true;
                }
                return true;
            }
            return false;
        }

        void setScript(Script script, IngestConditionalScript.Factory factory) {
            this.script = script;
            this.factory = factory;
        }
    }

    /*
     * This is meant to be used internally as the key of the map of samples
     */
    private record ProjectIndex(ProjectId projectId, String indexName) {};

}
