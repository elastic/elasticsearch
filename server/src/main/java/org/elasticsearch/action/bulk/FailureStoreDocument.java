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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

public class FailureStoreDocument {

    private final IndexRequest source;
    private final Exception exception;
    private final String targetIndexName;
    private final Supplier<Long> timeSupplier;

    public FailureStoreDocument(IndexRequest source, Exception exception, String targetIndexName) {
        this(source, exception, targetIndexName, System::currentTimeMillis);
    }

    public FailureStoreDocument(IndexRequest source, Exception exception, String targetIndexName, Supplier<Long> timeSupplier) {
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.exception = Objects.requireNonNull(exception, "exception must not be null");
        this.targetIndexName = Objects.requireNonNull(targetIndexName, "targetIndexName must not be null");
        this.timeSupplier = Objects.requireNonNull(timeSupplier, "timeSupplier must not be null");
    }

    public IndexRequest convert() throws IOException {
        // This is a problem - We want to target the targetted index name for creation, but we want the document to end up in its failure
        // store. We could target the failure store directly, but if it does not exist, then we need the auto create logic to somehow pick
        // up that the parent data stream needs to be created.
        // One option is to make use of the eventual flag to perform an operation on the failure store. Ughh who would have thought the
        // dependencies would be swapped like that...
        return new IndexRequest()
            .index(targetIndexName)
            .source(createSource())
            .setWriteToFailureStore(true);
    }

    private XContentBuilder createSource() throws IOException {
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
