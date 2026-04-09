/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.WrappingProcessor;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RerouteProcessorTests extends ESTestCase {

    public void testDefaults() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of(), List.of());
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "generic", "default");
    }

    public void testRouteOnType() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("event.type", "foo");

        RerouteProcessor processor = createRerouteProcessor(List.of("{{event.type}}"), List.of(), List.of());
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "foo", "generic", "default");
    }

    public void testEventDataset() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("event.dataset", "foo");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of("{{event.dataset }}"), List.of());
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "foo", "default");
        assertThat(ingestDocument.getFieldValue("event.dataset", String.class), equalTo("foo"));
    }

    public void testEventDatasetDottedFieldName() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.getCtxMap().put("event.dataset", "foo");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of("{{ event.dataset}}"), List.of());
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "foo", "default");
        assertThat(ingestDocument.getCtxMap().get("event.dataset"), equalTo("foo"));
        assertFalse(ingestDocument.getCtxMap().containsKey("event"));
    }

    public void testNoDataset() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("ds", "foo");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of("{{ ds }}"), List.of());
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "foo", "default");
        assertFalse(ingestDocument.hasField("event.dataset"));
    }

    public void testSkipFirstProcessor() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor skippedProcessor = createRerouteProcessor(List.of(), List.of("skip"), List.of());
        RerouteProcessor executedProcessor = createRerouteProcessor(List.of(), List.of("executed"), List.of());
        CompoundProcessor processor = new CompoundProcessor(new SkipProcessor(skippedProcessor), executedProcessor);
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "executed", "default");
    }

    public void testSkipLastProcessor() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor executedProcessor = createRerouteProcessor(List.of(), List.of("executed"), List.of());
        RerouteProcessor skippedProcessor = createRerouteProcessor(List.of(), List.of("skip"), List.of());
        CompoundProcessor processor = new CompoundProcessor(executedProcessor, skippedProcessor);
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "executed", "default");
    }

    public void testDataStreamFieldsFromDocument() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("data_stream.type", "eggplant");
        ingestDocument.setFieldValue("data_stream.dataset", "foo");
        ingestDocument.setFieldValue("data_stream.namespace", "bar");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of(), List.of());
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "eggplant", "foo", "bar");
    }

    public void testDataStreamFieldsFromDocumentDottedNotation() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.getCtxMap().put("data_stream.type", "eggplant");
        ingestDocument.getCtxMap().put("data_stream.dataset", "foo");
        ingestDocument.getCtxMap().put("data_stream.namespace", "bar");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of(), List.of());
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "eggplant", "foo", "bar");
    }

    public void testInvalidDataStreamFieldsFromDocument() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("data_stream.dataset", "foo-bar");
        ingestDocument.setFieldValue("data_stream.namespace", "baz#qux");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of(), List.of());
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

    public void testFieldReference() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("service.name", "opbeans-java");
        ingestDocument.setFieldValue("service.environment", "dev");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of("{{service.name}}"), List.of("{{service.environment}}"));
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "opbeans_java", "dev");
    }

    public void testRerouteToCurrentTarget() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor reroute = createRerouteProcessor(List.of(), List.of("generic"), List.of("default"));
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

        RerouteProcessor reroute = createRerouteProcessor(List.of(), List.of("{{service.name}}"), List.of("{{service.environment}}"));
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
        ingestDocument.setFieldValue("data_stream.dataset", "dataset_from_doc");
        ingestDocument.setFieldValue("data_stream.namespace", "namespace_from_doc");

        RerouteProcessor processor = createRerouteProcessor(
            List.of(),
            List.of("{{{data_stream.dataset}}}", "fallback"),
            List.of("{{data_stream.namespace}}", "fallback")
        );
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "dataset_from_doc", "namespace_from_doc");
    }

    public void testDatasetFieldReferenceMissingValue() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");

        RerouteProcessor processor = createRerouteProcessor(
            List.of(),
            List.of("{{data_stream.dataset}}", "fallback"),
            List.of("{{data_stream.namespace}}", "fallback")
        );
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "fallback", "fallback");
    }

    public void testDatasetFieldReference() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("data_stream.dataset", "generic");
        ingestDocument.setFieldValue("data_stream.namespace", "default");

        RerouteProcessor processor = createRerouteProcessor(
            List.of(),
            List.of("{{data_stream.dataset}}", "fallback"),
            List.of("{{{data_stream.namespace}}}", "fallback")
        );
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "generic", "default");
    }

    public void testFallbackToValuesFrom_index() throws Exception {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("data_stream.dataset", "foo");
        ingestDocument.setFieldValue("data_stream.namespace", "bar");

        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of("{{foo}}"), List.of("{{bar}}"));
        processor.execute(ingestDocument);
        assertDataSetFields(ingestDocument, "logs", "generic", "default");
    }

    public void testInvalidDataStreamName() throws Exception {
        {
            IngestDocument ingestDocument = createIngestDocument("foo");
            RerouteProcessor processor = createRerouteProcessor(List.of(), List.of(), List.of());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
            assertThat(e.getMessage(), equalTo("invalid data stream name: [foo]; must follow naming scheme <type>-<dataset>-<namespace>"));
        }

        {
            // naturally, though, a plain destination doesn't have to match the data stream naming convention
            IngestDocument ingestDocument = createIngestDocument("foo");
            RerouteProcessor processor = createRerouteProcessor("bar");
            processor.execute(ingestDocument);
            assertThat(ingestDocument.getFieldValue("_index", String.class), equalTo("bar"));
        }
    }

    public void testRouteOnNonStringFieldFails() {
        IngestDocument ingestDocument = createIngestDocument("logs-generic-default");
        ingestDocument.setFieldValue("numeric_field", 42);
        RerouteProcessor processor = createRerouteProcessor(List.of(), List.of("{{numeric_field}}"), List.of());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [numeric_field] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
    }

    public void testTypeSanitization() {
        assertTypeSanitization("\\/*?\"<>| ,#:-", "_____________");
        assertTypeSanitization("foo*bar", "foo_bar");
    }

    public void testDatasetSanitization() {
        assertDatasetSanitization("\\/*?\"<>| ,#:-", "_____________");
        assertDatasetSanitization("foo*bar", "foo_bar");
    }

    public void testNamespaceSanitization() {
        assertNamespaceSanitization("\\/*?\"<>| ,#:-", "____________-");
        assertNamespaceSanitization("foo*bar", "foo_bar");
    }

    private static void assertTypeSanitization(String type, String sanitizedType) {
        assertThat(
            RerouteProcessor.DataStreamValueSource.type("{{foo}}")
                .resolve(RandomDocumentPicks.randomIngestDocument(random(), Map.of("foo", type))),
            equalTo(sanitizedType)
        );
    }

    private static void assertDatasetSanitization(String dataset, String sanitizedDataset) {
        assertThat(
            RerouteProcessor.DataStreamValueSource.dataset("{{foo}}")
                .resolve(RandomDocumentPicks.randomIngestDocument(random(), Map.of("foo", dataset))),
            equalTo(sanitizedDataset)
        );
    }

    private static void assertNamespaceSanitization(String namespace, String sanitizedNamespace) {
        assertThat(
            RerouteProcessor.DataStreamValueSource.namespace("{{foo}}")
                .resolve(RandomDocumentPicks.randomIngestDocument(random(), Map.of("foo", namespace))),
            equalTo(sanitizedNamespace)
        );
    }

    private RerouteProcessor createRerouteProcessor(List<String> type, List<String> dataset, List<String> namespace) {
        return new RerouteProcessor(
            null,
            null,
            type.stream().map(RerouteProcessor.DataStreamValueSource::type).toList(),
            dataset.stream().map(RerouteProcessor.DataStreamValueSource::dataset).toList(),
            namespace.stream().map(RerouteProcessor.DataStreamValueSource::namespace).toList(),
            null
        );
    }

    private RerouteProcessor createRerouteProcessor(String destination) {
        return new RerouteProcessor(null, null, List.of(), List.of(), List.of(), destination);
    }

    private void assertDataSetFields(IngestDocument ingestDocument, String type, String dataset, String namespace) {
        if (ingestDocument.hasField("data_stream")) {
            assertThat(ingestDocument.getFieldValue("data_stream.type", String.class), equalTo(type));
            assertThat(ingestDocument.getFieldValue("data_stream.dataset", String.class), equalTo(dataset));
            assertThat(ingestDocument.getFieldValue("data_stream.namespace", String.class), equalTo(namespace));
        } else {
            assertThat(ingestDocument.getCtxMap().get("data_stream.type"), equalTo(type));
            assertThat(ingestDocument.getCtxMap().get("data_stream.dataset"), equalTo(dataset));
            assertThat(ingestDocument.getCtxMap().get("data_stream.namespace"), equalTo(namespace));
        }
        assertThat(ingestDocument.getFieldValue("_index", String.class), equalTo(type + "-" + dataset + "-" + namespace));
        if (ingestDocument.hasField("event.dataset")) {
            assertThat(
                ingestDocument.getFieldValue("event.dataset", String.class),
                equalTo(ingestDocument.getFieldValue("data_stream.dataset", String.class))
            );
        }
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
