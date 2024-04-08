/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Transforms an indexing request using error information into a new index request to be stored in a data stream's failure store.
 */
public class FailureStoreDocumentConverter {

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
            .source(createSource(source, exception, targetIndexName, timeSupplier))
            .opType(DocWriteRequest.OpType.CREATE)
            .setWriteToFailureStore(true);
    }

    private static XContentBuilder createSource(
        IndexRequest source,
        Exception exception,
        String targetIndexName,
        Supplier<Long> timeSupplier
    ) throws IOException {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(exception, "exception must not be null");
        Objects.requireNonNull(targetIndexName, "targetIndexName must not be null");
        Objects.requireNonNull(timeSupplier, "timeSupplier must not be null");
        Throwable unwrapped = ExceptionsHelper.unwrapCause(exception);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        {
            builder.timeField("@timestamp", timeSupplier.get());
            builder.startObject("document");
            {
                if (source.id() != null) {
                    builder.field("id", source.id());
                }
                if (source.routing() != null) {
                    builder.field("routing", source.routing());
                }
                builder.field("index", source.index());
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
                builder.field("stack_trace", ExceptionsHelper.stackTrace(unwrapped));
                // Further fields not yet tracked (Need to expose via specific exceptions)
                // - pipeline
                // - pipeline_trace
                // - processor
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
