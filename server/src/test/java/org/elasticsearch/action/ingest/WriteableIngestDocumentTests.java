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
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestIngestDocument;
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
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Predicate;

import static org.elasticsearch.ingest.IngestDocument.Metadata.VERSION;
import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.ingest.TestIngestDocument.randomVersion;
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
        sourceAndMetadata.put(VERSION.getFieldName(), randomVersion());
        int numFields = randomIntBetween(1, IngestDocument.Metadata.values().length);
        for (int i = 0; i < numFields; i++) {
            Tuple<String, Object> metadata = TestIngestDocument.randomMetadata();
            sourceAndMetadata.put(metadata.v1(), metadata.v2());
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

        for (String fieldName : ingestDocument.getMetadata().keySet()) {
            Object value = ingestDocument.getMetadata().get(fieldName);
            if (value == null) {
                assertThat(toXContentDoc.containsKey(fieldName), is(false));
            } else {
                assertThat(toXContentDoc.get(fieldName), equalTo(value.toString()));
            }
        }

        Map<String, Object> sourceAndMetadata = Maps.newMapWithExpectedSize(toXContentSource.size() + ingestDocument.getMetadata().size());
        sourceAndMetadata.putAll(toXContentSource);
        ingestDocument.getMetadata().keySet().forEach(k -> sourceAndMetadata.put(k, ingestDocument.getMetadata().get(k)));
        IngestDocument serializedIngestDocument = new IngestDocument(sourceAndMetadata, toXContentIngestMetadata);
        assertIngestDocument(writeableIngestDocument.getIngestDocument(), serializedIngestDocument);
    }

    public void testXContentHashSetSerialization() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Map.of("key", Set.of("value")));
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
