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

import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Processor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Processor used for testing, keeps track of how many times it is invoked and
 * accepts a {@link Consumer} of {@link IngestDocument} to be called when executed.
 */
public class TestProcessor implements Processor {

    private final String type;
    private final String tag;
    private final Consumer<IngestDocument> ingestDocumentConsumer;
    private final AtomicInteger invokedCounter = new AtomicInteger();

    public TestProcessor(Consumer<IngestDocument> ingestDocumentConsumer) {
        this(null, "test-processor", ingestDocumentConsumer);
    }

    public TestProcessor(String tag, String type, Consumer<IngestDocument> ingestDocumentConsumer) {
        this.ingestDocumentConsumer = ingestDocumentConsumer;
        this.type = type;
        this.tag = tag;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        invokedCounter.incrementAndGet();
        ingestDocumentConsumer.accept(ingestDocument);
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getTag() {
        return tag;
    }

    public int getInvokedCounter() {
        return invokedCounter.get();
    }

    public static final class Factory extends AbstractProcessorFactory<TestProcessor> {
        @Override
        public TestProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            return new TestProcessor(processorTag, "test-processor", ingestDocument -> {});
        }
    }
}
