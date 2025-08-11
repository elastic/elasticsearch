/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Set;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Predicate;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class WriteableIngestDocumentTests extends AbstractXContentTestCase<WriteableIngestDocument> {

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    @Override
    protected void assertEqualInstances(WriteableIngestDocument expectedInstance, WriteableIngestDocument newInstance) {
        assertIngestDocument(expectedInstance.getIngestDocument(), newInstance.getIngestDocument());
    }

    public void testSerialization() throws IOException {
        Map<String, Object> sourceAndMetadata = RandomDocumentPicks.randomSource(random());
        int numFields = randomIntBetween(1, IngestDocument.Metadata.values().length);
        for (int i = 0; i < numFields; i++) {
            sourceAndMetadata.put(randomFrom(IngestDocument.Metadata.values()).getFieldName(), randomAlphaOfLengthBetween(5, 10));
        }
        Map<String, Object> ingestMetadata = new HashMap<>();
        numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            ingestMetadata.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        WriteableIngestDocument writeableIngestDocument = new WriteableIngestDocument(
            new IngestDocument(sourceAndMetadata, ingestMetadata)
        );

        BytesStreamOutput out = new BytesStreamOutput();
        writeableIngestDocument.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        WriteableIngestDocument otherWriteableIngestDocument = new WriteableIngestDocument(streamInput);
        assertIngestDocument(otherWriteableIngestDocument.getIngestDocument(), writeableIngestDocument.getIngestDocument());
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws IOException {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        WriteableIngestDocument writeableIngestDocument = new WriteableIngestDocument(new IngestDocument(ingestDocument));

        // using a cbor builder here, so that byte arrays do not get converted, so equalTo() below works
        XContentBuilder builder = XContentFactory.cborBuilder();
        builder.startObject();
        writeableIngestDocument.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        Map<String, Object> toXContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        Map<String, Object> toXContentDoc = (Map<String, Object>) toXContentMap.get("doc");
        Map<String, Object> toXContentSource = (Map<String, Object>) toXContentDoc.get("_source");
        Map<String, Object> toXContentIngestMetadata = (Map<String, Object>) toXContentDoc.get("_ingest");

        Map<IngestDocument.Metadata, Object> metadataMap = ingestDocument.extractMetadata();
        for (Map.Entry<IngestDocument.Metadata, Object> metadata : metadataMap.entrySet()) {
            String fieldName = metadata.getKey().getFieldName();
            if (metadata.getValue() == null) {
                assertThat(toXContentDoc.containsKey(fieldName), is(false));
            } else {
                assertThat(toXContentDoc.get(fieldName), equalTo(metadata.getValue().toString()));
            }
        }

        IngestDocument serializedIngestDocument = new IngestDocument(toXContentSource, toXContentIngestMetadata);
        assertIngestDocument(serializedIngestDocument, serializedIngestDocument);
    }

    public void testXContentHashSetSerialization() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            org.elasticsearch.core.Map.of("key", Set.of("value"))
        );
        final WriteableIngestDocument writeableIngestDocument = new WriteableIngestDocument(ingestDocument);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            writeableIngestDocument.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertThat(map.get("doc"), is(instanceOf(Map.class)));
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) ((Map) map.get("doc")).get("_source");
            assertThat(source.get("key"), is(Arrays.asList("value")));
        }
    }

    public void testCopiesTheIngestDocument() {
        IngestDocument document = createRandomIngestDoc();
        WriteableIngestDocument wid = new WriteableIngestDocument(document);

        assertIngestDocument(wid.getIngestDocument(), document);
        assertThat(wid.getIngestDocument(), not(sameInstance(document)));
    }

    static IngestDocument createRandomIngestDoc() {
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference sourceBytes = RandomObjects.randomSource(random(), xContentType);
        Map<String, Object> randomSource = XContentHelper.convertToMap(sourceBytes, false, xContentType).v2();
        return RandomDocumentPicks.randomIngestDocument(random(), randomSource);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected WriteableIngestDocument createTestInstance() {
        return new WriteableIngestDocument(createRandomIngestDoc());
    }

    @Override
    protected WriteableIngestDocument doParseInstance(XContentParser parser) {
        return WriteableIngestDocument.fromXContent(parser);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // We cannot have random fields in the _source field and _ingest field
        return field -> field.startsWith(
            new StringJoiner(".").add(WriteableIngestDocument.DOC_FIELD).add(WriteableIngestDocument.SOURCE_FIELD).toString()
        )
            || field.startsWith(
                new StringJoiner(".").add(WriteableIngestDocument.DOC_FIELD).add(WriteableIngestDocument.INGEST_FIELD).toString()
            );
    }
}
