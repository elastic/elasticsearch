/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.annotations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Persists annotations to Elasticsearch index.
 */
public class AnnotationPersister {

    private static final Logger logger = LogManager.getLogger(AnnotationPersister.class);

    private static final int DEFAULT_BULK_LIMIT = 10_000;

    private final ResultsPersisterService resultsPersisterService;

    /**
     * Execute bulk requests when they reach this size
     */
    private final int bulkLimit;

    public AnnotationPersister(ResultsPersisterService resultsPersisterService) {
        this(resultsPersisterService, DEFAULT_BULK_LIMIT);
    }

    // For testing
    AnnotationPersister(ResultsPersisterService resultsPersisterService, int bulkLimit) {
        this.resultsPersisterService = Objects.requireNonNull(resultsPersisterService);
        this.bulkLimit = bulkLimit;
    }

    /**
     * Persists the given annotation to annotations index.
     *
     * @param annotationId existing annotation id. If {@code null}, a new annotation will be created and id will be assigned automatically
     * @param annotation annotation to be persisted
     * @return tuple of the form (annotation id, annotation object)
     */
    public Tuple<String, Annotation> persistAnnotation(@Nullable String annotationId, Annotation annotation) {
        Objects.requireNonNull(annotation);
        String jobId = annotation.getJobId();
        BulkResponse bulkResponse = bulkPersisterBuilder(jobId).persistAnnotation(annotationId, annotation).executeRequest();
        assert bulkResponse.getItems().length == 1;
        return Tuple.tuple(bulkResponse.getItems()[0].getId(), annotation);
    }

    public Builder bulkPersisterBuilder(String jobId) {
        return new Builder(jobId, () -> true);
    }

    public Builder bulkPersisterBuilder(String jobId, Supplier<Boolean> shouldRetry) {
        return new Builder(jobId, shouldRetry);
    }

    public class Builder {

        private final String jobId;
        private BulkRequest bulkRequest = new BulkRequest(AnnotationIndex.WRITE_ALIAS_NAME);
        private final Supplier<Boolean> shouldRetry;

        private Builder(String jobId, Supplier<Boolean> shouldRetry) {
            this.jobId = Objects.requireNonNull(jobId);
            this.shouldRetry = Objects.requireNonNull(shouldRetry);
        }

        public Builder persistAnnotation(Annotation annotation) {
            return persistAnnotation(null, annotation);
        }

        public Builder persistAnnotation(@Nullable String annotationId, Annotation annotation) {
            Objects.requireNonNull(annotation);
            try (XContentBuilder xContentBuilder = annotation.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {
                bulkRequest.add(new IndexRequest().id(annotationId).source(xContentBuilder).setRequireAlias(true));
            } catch (IOException e) {
                logger.error(() -> "[" + jobId + "] Error serialising annotation", e);
            }

            if (bulkRequest.numberOfActions() >= bulkLimit) {
                executeRequest();
            }
            return this;
        }

        /**
         * Execute the bulk action
         */
        public BulkResponse executeRequest() {
            if (bulkRequest.numberOfActions() == 0) {
                return null;
            }
            logger.trace("[{}] ES API CALL: bulk request with {} actions", () -> jobId, () -> bulkRequest.numberOfActions());
            BulkResponse bulkResponse = resultsPersisterService.bulkIndexWithRetry(
                bulkRequest,
                jobId,
                shouldRetry,
                retryMessage -> logger.debug("[{}] Bulk indexing of annotations failed {}", jobId, retryMessage)
            );
            bulkRequest = new BulkRequest(AnnotationIndex.WRITE_ALIAS_NAME);
            return bulkResponse;
        }
    }
}
