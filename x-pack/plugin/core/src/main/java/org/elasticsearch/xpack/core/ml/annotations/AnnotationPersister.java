/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.annotations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Persists annotations to Elasticsearch index.
 */
public class AnnotationPersister {

    private static final Logger logger = LogManager.getLogger(AnnotationPersister.class);

    private final Client client;
    private final AbstractAuditor auditor;

    public AnnotationPersister(Client client, AbstractAuditor auditor) {
        this.client = Objects.requireNonNull(client);
        this.auditor = Objects.requireNonNull(auditor);
    }

    /**
     * Persists the given annotation to annotations index.
     *
     * @param annotationId existing annotation id. If {@code null}, a new annotation will be created and id will be assigned automatically
     * @param annotation annotation to be persisted
     * @param errorMessage error message to report when annotation fails to be persisted
     * @return tuple of the form (annotation id, annotation object)
     */
    public Tuple<String, Annotation> persistAnnotation(@Nullable String annotationId, Annotation annotation, String errorMessage) {
        Objects.requireNonNull(annotation);
        try (XContentBuilder xContentBuilder = annotation.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {
            IndexRequest indexRequest =
                new IndexRequest(AnnotationIndex.WRITE_ALIAS_NAME)
                    .id(annotationId)
                    .source(xContentBuilder);
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
                IndexResponse response = client.index(indexRequest).actionGet();
                return Tuple.tuple(response.getId(), annotation);
            }
        } catch (IOException ex) {
            String jobId = annotation.getJobId();
            logger.error(errorMessage, ex);
            auditor.error(jobId, errorMessage);
            return null;
        }
    }
}
