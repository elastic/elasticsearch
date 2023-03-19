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
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.ingest.WrappingProcessor;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class RerouteProcessorTests extends ESTestCase {

    public void testDefaults() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of());
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "generic", "default");
    }

    public void testSkipFirstProcessor() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor skippedProcessor = createRerouteProcessor(List.of("skip"), List.of());
        RerouteProcessor executedProcessor = createRerouteProcessor(List.of("executed"), List.of());
        CompoundProcessor processor = new CompoundProcessor(new SkipProcessor(skippedProcessor), executedProcessor);
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "executed", "default");
    }

    public void testSkipLastProcessor() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor executedProcessor = createRerouteProcessor(List.of("executed"), List.of());
        RerouteProcessor skippedProcessor = createRerouteProcessor(List.of("skip"), List.of());
        CompoundProcessor processor = new CompoundProcessor(executedProcessor, skippedProcessor);
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "executed", "default");
    }

    public void testDataStreamFieldsFromDocument() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("data_stream.dataset", "foo");
        ingestDocument.setFieldValue("data_stream.namespace", "bar");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of());
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "foo", "bar");
    }

    public void testInvalidDataStreamFieldsFromDocument() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("data_stream.dataset", "foo-bar");
        ingestDocument.setFieldValue("data_stream.namespace", "baz#qux");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of());
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "foo_bar", "baz_qux");
    }

    public void testDestination() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor processor = createRerouteProcessor("foo");
        processor.execute(ingestDocument);
        assertFalse(ingestDocument.hasField("data_stream"));
        assertThat(ingestDocument.getFieldValue("_index", String.class), equalTo("foo"));
    }

    public void testRerouteToCurrentTarget() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor reroute = createRerouteProcessor(List.of("generic"), List.of("default"));
        CompoundProcessor processor = new CompoundProcessor(
            reroute,
            new TestProcessor(doc -> doc.setFieldValue("pipeline_is_continued", true))
        );
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "generic", "default");
        assertFalse(ingestDocument.hasField("pipeline_is_continued"));
    }

    public void testFieldReferenceWithMissingReroutesToCurrentTarget() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor reroute = createRerouteProcessor(List.of(""), List.of(""));
        CompoundProcessor processor = new CompoundProcessor(
            reroute,
            new TestProcessor(doc -> doc.setFieldValue("pipeline_is_continued", true))
        );
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("_index", String.class), equalTo("logs-generic-default"));
        assertDataSetFields(ingestDocument, "logs", "generic", "default");
        assertFalse(ingestDocument.hasField("pipeline_is_continued"));
    }

    public void testDataStreamFieldReference() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor processor = createRerouteProcessor(
            List.of("dataset_from_doc", "fallback"),
            List.of("namespace_from_doc", "fallback")
        );
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "dataset_from_doc", "namespace_from_doc");
    }

    public void testDatasetFieldReferenceMissingValue() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor processor = createRerouteProcessor(
            List.of("", "fallback"),
            List.of("", "fallback")
        );
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "fallback", "fallback");
    }

    public void testDatasetFieldReference() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor processor = createRerouteProcessor(
            List.of("generic", "fallback"),
            List.of("default", "fallback")
        );
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "generic", "default");
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

    private RerouteProcessor createRerouteProcessor(String destination) {
        return new RerouteProcessor(new TestTemplateService.MockTemplateScript.Factory(destination));
    }

    private RerouteProcessor createRerouteProcessor(List<String> dataset, List<String> namespace) {
        return new RerouteProcessor(asTemplate(dataset), asTemplate(namespace));
    }

    private static List<TemplateScript.Factory> asTemplate(List<String> dataset) {
        return dataset.stream().map(TestTemplateService.MockTemplateScript.Factory::new).collect(Collectors.toList());
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
