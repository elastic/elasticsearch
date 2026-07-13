/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.cluster.metadata.ProjectId;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Processor used for testing, keeps track of how many times it is invoked and
 * accepts a {@link Consumer} of {@link IngestDocument} to be called when executed.
 */
public class TestProcessor implements Processor {

    private final String type;
    private final String tag;
    private final String description;
    private final Function<IngestDocument, IngestDocument> ingestDocumentMapper;
    private final AtomicInteger invokedCounter = new AtomicInteger();

    public TestProcessor(Consumer<IngestDocument> ingestDocumentConsumer) {
        this(null, "test-processor", null, ingestDocumentConsumer);
    }

    public TestProcessor(RuntimeException e) {
        this(null, "test-processor", null, e);
    }

    public TestProcessor(String tag, String type, String description, RuntimeException e) {
        this(tag, type, description, (Consumer<IngestDocument>) i -> { throw e; });
    }

    public TestProcessor(String tag, String type, String description, Consumer<IngestDocument> ingestDocumentConsumer) {
        this(tag, type, description, id -> {
            ingestDocumentConsumer.accept(id);
            return id;
        });
    }

    public TestProcessor(String tag, String type, String description, Function<IngestDocument, IngestDocument> ingestDocumentMapper) {
        this.ingestDocumentMapper = ingestDocumentMapper;
        this.type = type;
        this.tag = tag;
        this.description = description;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        invokedCounter.incrementAndGet();

        try {
            ingestDocumentMapper.apply(ingestDocument);
        } catch (Exception e) {
            if (this.isAsync()) {
                handler.accept(null, e);
                return;
            } else {
                throw e;
            }
        }

        handler.accept(ingestDocument, null);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        invokedCounter.incrementAndGet();
        return ingestDocumentMapper.apply(ingestDocument);
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public int getInvokedCounter() {
        return invokedCounter.get();
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public TestProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            return new TestProcessor(processorTag, "test-processor", description, ingestDocument -> {});
        }
    }
}
