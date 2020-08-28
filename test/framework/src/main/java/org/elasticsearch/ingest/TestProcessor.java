/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
        public TestProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                    String description, Map<String, Object> config) throws Exception {
            return new TestProcessor(processorTag, "test-processor", description, ingestDocument -> {});
        }
    }
}
