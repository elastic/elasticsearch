/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.WrappingProcessor;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RerouteProcessorTests extends ESTestCase {

    public void testDefaults() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor processor = new RerouteProcessor(null, null, null, null, null);
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "generic", "default");
    }

    public void testSkipFirstProcessor() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor skippedProcessor = new RerouteProcessor(null, null, "skip", null, null);
        RerouteProcessor executedProcessor = new RerouteProcessor(null, null, "executed", null, null);
        CompoundProcessor processor = new CompoundProcessor(new SkipProcessor(skippedProcessor), executedProcessor);
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "executed", "default");
    }

    public void testSkipLastProcessor() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor executedProcessor = new RerouteProcessor(null, null, "executed", null, null);
        RerouteProcessor skippedProcessor = new RerouteProcessor(null, null, "skip", null, null);
        CompoundProcessor processor = new CompoundProcessor(executedProcessor, skippedProcessor);
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "executed", "default");
    }

    public void testDataStreamFieldsFromDocument() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("data_stream.dataset", "foo");
        ingestDocument.setFieldValue("data_stream.namespace", "bar");

        RerouteProcessor processor = new RerouteProcessor(null, null, null, null, null);
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "foo", "bar");
    }

    public void testInvalidDataStreamFieldsFromDocument() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("data_stream.dataset", "foo-bar");
        ingestDocument.setFieldValue("data_stream.namespace", "baz#qux");

        RerouteProcessor processor = new RerouteProcessor(null, null, null, null, null);
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "foo_bar", "baz_qux");
    }

    public void testDestination() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor processor = new RerouteProcessor(null, null, null, null, "foo");
        processor.execute(ingestDocument);
        assertFalse(ingestDocument.hasField("data_stream"));
        assertThat(ingestDocument.getFieldValue("_index", String.class), equalTo("foo"));
    }

    private void assertDataSetFields(IngestDocument ingestDocument, String type, String dataset, String namespace) {
        assertThat(ingestDocument.getFieldValue("data_stream.type", String.class), equalTo(type));
        assertThat(ingestDocument.getFieldValue("data_stream.dataset", String.class), equalTo(dataset));
        assertThat(ingestDocument.getFieldValue("data_stream.namespace", String.class), equalTo(namespace));
        assertThat(ingestDocument.getFieldValue("_index", String.class), equalTo(type + "-" + dataset + "-" + namespace));
    }

    private static IngestDocument createIngestDocument(String dataStream) {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        ingestDocument.setFieldValue("_index", dataStream);
        return ingestDocument;
    }

    private static class SkipProcessor implements WrappingProcessor {
        private final Processor processor;

        SkipProcessor(Processor processor) {
            this.processor = processor;
        }

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            return ingestDocument;
        }

        @Override
        public Processor getInnerProcessor() {
            return processor;
        }

        @Override
        public String getType() {
            return "skip";
        }

        @Override
        public String getTag() {
            return null;
        }

        @Override
        public String getDescription() {
            return null;
        }
    }
}
