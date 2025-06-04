/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentElasticsearchExtension.DEFAULT_FORMATTER;
import static org.elasticsearch.ingest.CompoundProcessor.PIPELINE_ORIGIN_EXCEPTION_HEADER;
import static org.elasticsearch.ingest.CompoundProcessor.PROCESSOR_TAG_EXCEPTION_HEADER;
import static org.elasticsearch.ingest.CompoundProcessor.PROCESSOR_TYPE_EXCEPTION_HEADER;

/**
 * Transforms an indexing request using error information into a new index request to be stored in a data stream's failure store.
 */
public class FailureStoreDocumentConverter {

    private static final int STACKTRACE_PRINT_DEPTH = 2;

    private static final Set<String> INGEST_EXCEPTION_HEADERS = Set.of(
        PIPELINE_ORIGIN_EXCEPTION_HEADER,
        PROCESSOR_TAG_EXCEPTION_HEADER,
        PROCESSOR_TYPE_EXCEPTION_HEADER
    );

    /**
     * Combines an {@link IndexRequest} that has failed during the bulk process with the error thrown for that request. The result is a
     * new {@link IndexRequest} that can be stored in a data stream's failure store.
     * @param source The original request that has failed to be ingested
     * @param exception The exception that was thrown that caused the request to fail to be ingested
     * @param targetIndexName The index that the request was targeting at time of failure
     * @return A new {@link IndexRequest} with a failure store compliant structure
     * @throws IOException If there is a problem when the document's new source is serialized
     */
    public IndexRequest transformFailedRequest(IndexRequest source, Exception exception, String targetIndexName) throws IOException {
        return transformFailedRequest(source, exception, targetIndexName, System::currentTimeMillis);
    }

    /**
     * Combines an {@link IndexRequest} that has failed during the bulk process with the error thrown for that request. The result is a
     * new {@link IndexRequest} that can be stored in a data stream's failure store.
     * @param source The original request that has failed to be ingested
     * @param exception The exception that was thrown that caused the request to fail to be ingested
     * @param targetIndexName The index that the request was targeting at time of failure
     * @param timeSupplier Supplies the value for the document's timestamp
     * @return A new {@link IndexRequest} with a failure store compliant structure
     * @throws IOException If there is a problem when the document's new source is serialized
     */
    public IndexRequest transformFailedRequest(
        IndexRequest source,
        Exception exception,
        String targetIndexName,
        Supplier<Long> timeSupplier
    ) throws IOException {
        return new IndexRequest().index(targetIndexName)
            .source(createSource(source, exception, timeSupplier))
            .opType(DocWriteRequest.OpType.CREATE)
            .setWriteToFailureStore(true);
    }

    private static XContentBuilder createSource(IndexRequest source, Exception exception, Supplier<Long> timeSupplier) throws IOException {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(exception, "exception must not be null");
        Objects.requireNonNull(timeSupplier, "timeSupplier must not be null");
        Throwable unwrapped = ExceptionsHelper.unwrapCause(exception);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        {
            builder.field("@timestamp", DEFAULT_FORMATTER.format(Instant.ofEpochMilli(timeSupplier.get())));
            builder.startObject("document");
            {
                if (source.id() != null) {
                    builder.field("id", source.id());
                }
                if (source.routing() != null) {
                    builder.field("routing", source.routing());
                }
                if (source.index() != null) {
                    builder.field("index", source.index());
                }
                // Unmapped source field
                builder.startObject("source");
                {
                    builder.mapContents(source.sourceAsMap());
                }
                builder.endObject();
            }
            builder.endObject();
            builder.startObject("error");
            {
                builder.field("type", ElasticsearchException.getExceptionName(unwrapped));
                builder.field("message", unwrapped.getMessage());
                builder.field("stack_trace", ExceptionsHelper.limitedStackTrace(unwrapped, STACKTRACE_PRINT_DEPTH));
                // Try to find the IngestProcessorException somewhere in the stack trace. Since IngestProcessorException is package-private,
                // we can't instantiate it in tests, so we'll have to check for the headers directly.
                var ingestException = ExceptionsHelper.<ElasticsearchException>unwrapCausesAndSuppressed(
                    exception,
                    t -> t instanceof ElasticsearchException e && Sets.haveNonEmptyIntersection(e.getHeaderKeys(), INGEST_EXCEPTION_HEADERS)
                ).orElse(null);
                if (ingestException != null) {
                    if (ingestException.getHeaderKeys().contains(PIPELINE_ORIGIN_EXCEPTION_HEADER)) {
                        List<String> pipelineOrigin = ingestException.getHeader(PIPELINE_ORIGIN_EXCEPTION_HEADER);
                        Collections.reverse(pipelineOrigin);
                        if (pipelineOrigin.isEmpty() == false) {
                            builder.field("pipeline_trace", pipelineOrigin);
                            builder.field("pipeline", pipelineOrigin.get(pipelineOrigin.size() - 1));
                        }
                    }
                    if (ingestException.getHeaderKeys().contains(PROCESSOR_TAG_EXCEPTION_HEADER)) {
                        builder.field("processor_tag", ingestException.getHeader(PROCESSOR_TAG_EXCEPTION_HEADER).get(0));
                    }
                    if (ingestException.getHeaderKeys().contains(PROCESSOR_TYPE_EXCEPTION_HEADER)) {
                        builder.field("processor_type", ingestException.getHeader(PROCESSOR_TYPE_EXCEPTION_HEADER).get(0));
                    }
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
