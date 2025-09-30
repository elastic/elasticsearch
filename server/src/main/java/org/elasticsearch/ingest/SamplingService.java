/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class SamplingService implements ClusterStateListener {
    public static final boolean RANDOM_SAMPLING_FEATURE_FLAG = new FeatureFlag("random_sampling").isEnabled();
    private static final Logger logger = LogManager.getLogger(SamplingService.class);
    private final ScriptService scriptService;
    private final ClusterService clusterService;

    public SamplingService(ScriptService scriptService, ClusterService clusterService) {
        this.scriptService = scriptService;
        this.clusterService = clusterService;
    }

    /**
     * Potentially samples the given indexRequest, depending on the existing sampling configuration.
     * @param projectMetadata Used to get the sampling configuration
     * @param indexRequest The raw request to potentially sample
     */
    public void maybeSample(ProjectMetadata projectMetadata, IndexRequest indexRequest) {
        maybeSample(projectMetadata, indexRequest.index(), indexRequest, () -> {
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
     *
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
        // TODO Sampling logic to go here in the near future
    }

    public List<RawDocument> getLocalSample(ProjectId projectId, String index) {
        return List.of(); // TODO placeholder
    }

    public boolean atLeastOneSampleConfigured() {
        return false; // TODO Return true if there is at least one sample in the cluster state
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO: React to sampling config changes
    }

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

    private RawDocument getRawDocumentForIndexRequest(ProjectId projectId, String indexName, IndexRequest indexRequest) {
        BytesReference sourceReference = indexRequest.source();
        assert sourceReference != null : "Cannot sample an IndexRequest with no source";
        byte[] source = sourceReference.array();
        final byte[] sourceCopy = new byte[sourceReference.length()];
        System.arraycopy(source, sourceReference.arrayOffset(), sourceCopy, 0, sourceReference.length());
        return new RawDocument(projectId, indexName, sourceCopy, indexRequest.getContentType());
    }

}
