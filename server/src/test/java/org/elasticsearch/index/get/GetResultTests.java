/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.get;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.get.DocumentFieldTests.randomDocumentField;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class GetResultTests extends ESTestCase {

    public static GetResult parseInstance(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        return parseInstanceFromEmbedded(parser);
    }

    public static GetResult parseInstanceFromEmbedded(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        return parseInstanceFromEmbedded(parser, null, null);
    }

    public static GetResult parseInstanceFromEmbedded(XContentParser parser, String index, String id) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();
        long version = -1;
        long seqNo = UNASSIGNED_SEQ_NO;
        long primaryTerm = UNASSIGNED_PRIMARY_TERM;
        Boolean found = null;
        BytesReference source = null;
        Map<String, DocumentField> documentFields = new HashMap<>();
        Map<String, DocumentField> metaFields = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (GetResult._INDEX.equals(currentFieldName)) {
                    index = parser.text();
                } else if (GetResult._ID.equals(currentFieldName)) {
                    id = parser.text();
                } else if (GetResult._VERSION.equals(currentFieldName)) {
                    version = parser.longValue();
                } else if (GetResult._SEQ_NO.equals(currentFieldName)) {
                    seqNo = parser.longValue();
                } else if (GetResult._PRIMARY_TERM.equals(currentFieldName)) {
                    primaryTerm = parser.longValue();
                } else if (GetResult.FOUND.equals(currentFieldName)) {
                    found = parser.booleanValue();
                } else {
                    metaFields.put(currentFieldName, new DocumentField(currentFieldName, singletonList(parser.objectText())));
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SourceFieldMapper.NAME.equals(currentFieldName)) {
                    try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
                        // the original document gets slightly modified: whitespaces or pretty printing are not preserved,
                        // it all depends on the current builder settings
                        builder.copyCurrentStructure(parser);
                        source = BytesReference.bytes(builder);
                    }
                } else if (GetResult.FIELDS.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        DocumentField getField = DocumentField.fromXContent(parser);
                        documentFields.put(getField.getName(), getField);
                    }
                } else {
                    parser.skipChildren(); // skip potential inner objects for forward compatibility
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (IgnoredFieldMapper.NAME.equals(currentFieldName) || IgnoredSourceFieldMapper.NAME.equals(currentFieldName)) {
                    metaFields.put(currentFieldName, new DocumentField(currentFieldName, parser.list()));
                } else {
                    parser.skipChildren(); // skip potential inner arrays for forward compatibility
                }
            }
        }
        return new GetResult(index, id, seqNo, primaryTerm, version, found, source, documentFields, metaFields);
    }

    public void testToAndFromXContent() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<GetResult, GetResult> tuple = randomGetResult(xContentType);
        GetResult getResult = tuple.v1();
        GetResult expectedGetResult = tuple.v2();
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(getResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable, "_source");
        // test that we can parse what we print out
        GetResult parsedGetResult;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedGetResult = parseInstance(parser);
            assertNull(parser.nextToken());
        }
        assertEquals(expectedGetResult, parsedGetResult);
        // print the parsed object out and test that the output is the same as the original output
        BytesReference finalBytes = toXContent(parsedGetResult, xContentType, humanReadable);
        assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
        // check that the source stays unchanged, no shuffling of keys nor anything like that
        assertEquals(expectedGetResult.sourceAsString(), parsedGetResult.sourceAsString());
    }

    public void testToXContent() throws IOException {
        {
            GetResult getResult = new GetResult(
                "index",
                "id",
                0,
                1,
                1,
                true,
                new BytesArray("""
                    { "field1" : "value1", "field2":"value2"}"""),
                singletonMap("field1", new DocumentField("field1", singletonList("value1"))),
                singletonMap("field1", new DocumentField("metafield", singletonList("metavalue")))
            );
            String output = Strings.toString(getResult);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "_index": "index",
                  "_id": "id",
                  "_version": 1,
                  "_seq_no": 0,
                  "_primary_term": 1,
                  "metafield": "metavalue",
                  "found": true,
                  "_source": {
                    "field1": "value1",
                    "field2": "value2"
                  },
                  "fields": {
                    "field1": [ "value1" ]
                  }
                }"""), XContentHelper.stripWhitespace(output));
        }
        {
            GetResult getResult = new GetResult("index", "id", UNASSIGNED_SEQ_NO, 0, 1, false, null, null, null);
            String output = Strings.toString(getResult);
            assertEquals("""
                {"_index":"index","_id":"id","found":false}""", output);
        }
    }

    public void testToAndFromXContentEmbedded() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<GetResult, GetResult> tuple = randomGetResult(xContentType);
        GetResult getResult = tuple.v1();
        // We don't expect to retrieve the index/type/id of the GetResult because they are not rendered
        // by the toXContentEmbedded method.
        GetResult expectedGetResult = new GetResult(
            null,
            null,
            tuple.v2().getSeqNo(),
            tuple.v2().getPrimaryTerm(),
            -1,
            tuple.v2().isExists(),
            tuple.v2().sourceRef(),
            tuple.v2().getDocumentFields(),
            tuple.v2().getMetadataFields()
        );

        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toXContentEmbedded(getResult, xContentType, humanReadable);

        // Test that we can parse the result of toXContentEmbedded()
        GetResult parsedEmbeddedGetResult;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsedEmbeddedGetResult = parseInstanceFromEmbedded(parser);
            assertNull(parser.nextToken());
        }
        assertEquals(expectedGetResult, parsedEmbeddedGetResult);
        // print the parsed object out and test that the output is the same as the original output
        BytesReference finalBytes = toXContentEmbedded(parsedEmbeddedGetResult, xContentType, humanReadable);
        assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
        // check that the source stays unchanged, no shuffling of keys nor anything like that
        assertEquals(expectedGetResult.sourceAsString(), parsedEmbeddedGetResult.sourceAsString());
    }

    public void testToXContentEmbedded() throws IOException {
        Map<String, DocumentField> fields = new HashMap<>();
        fields.put("foo", new DocumentField("foo", singletonList("bar")));
        fields.put("baz", new DocumentField("baz", Arrays.asList("baz_0", "baz_1")));

        GetResult getResult = new GetResult(
            "index",
            "id",
            0,
            1,
            2,
            true,
            new BytesArray("{\"foo\":\"bar\",\"baz\":[\"baz_0\",\"baz_1\"]}"),
            fields,
            null
        );

        BytesReference originalBytes = toXContentEmbedded(getResult, XContentType.JSON, false);
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_seq_no": 0,
              "_primary_term": 1,
              "found": true,
              "_source": {
                "foo": "bar",
                "baz": [ "baz_0", "baz_1" ]
              },
              "fields": {
                "foo": [ "bar" ],
                "baz": [ "baz_0", "baz_1" ]
              }
            }"""), originalBytes.utf8ToString());
    }

    public void testToXContentEmbeddedNotFound() throws IOException {
        GetResult getResult = new GetResult("index", "id", UNASSIGNED_SEQ_NO, 0, 1, false, null, null, null);

        BytesReference originalBytes = toXContentEmbedded(getResult, XContentType.JSON, false);
        assertEquals("{\"found\":false}", originalBytes.utf8ToString());
    }

    public void testSerializationNotFound() throws IOException {
        // serializes and deserializes with streamable, then prints back to xcontent
        GetResult getResult = new GetResult("index", "id", UNASSIGNED_SEQ_NO, 0, 1, false, null, null, null);

        BytesStreamOutput out = new BytesStreamOutput();
        getResult.writeTo(out);
        getResult = new GetResult(out.bytes().streamInput());

        BytesReference originalBytes = toXContentEmbedded(getResult, XContentType.JSON, false);
        assertEquals("{\"found\":false}", originalBytes.utf8ToString());
    }

    public void testGetSourceAsBytes() {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<GetResult, GetResult> tuple = randomGetResult(xContentType);
        GetResult getResult = tuple.v1();
        if (getResult.isExists() && getResult.isSourceEmpty() == false) {
            assertNotNull(getResult.sourceRef());
        } else {
            assertNull(getResult.sourceRef());
        }
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(randomGetResult(XContentType.JSON).v1(), GetResultTests::copyGetResult, GetResultTests::mutateGetResult);
    }

    public static GetResult copyGetResult(GetResult getResult) {
        return new GetResult(
            getResult.getIndex(),
            getResult.getId(),
            getResult.getSeqNo(),
            getResult.getPrimaryTerm(),
            getResult.getVersion(),
            getResult.isExists(),
            getResult.internalSourceRef(),
            getResult.getDocumentFields(),
            getResult.getMetadataFields()
        );
    }

    public static GetResult mutateGetResult(GetResult getResult) {
        List<Supplier<GetResult>> mutations = new ArrayList<>();
        mutations.add(
            () -> new GetResult(
                randomUnicodeOfLength(15),
                getResult.getId(),
                getResult.getSeqNo(),
                getResult.getPrimaryTerm(),
                getResult.getVersion(),
                getResult.isExists(),
                getResult.internalSourceRef(),
                getResult.getFields(),
                null
            )
        );
        mutations.add(
            () -> new GetResult(
                getResult.getIndex(),
                randomUnicodeOfLength(15),
                getResult.getSeqNo(),
                getResult.getPrimaryTerm(),
                getResult.getVersion(),
                getResult.isExists(),
                getResult.internalSourceRef(),
                getResult.getFields(),
                null
            )
        );
        mutations.add(
            () -> new GetResult(
                getResult.getIndex(),
                getResult.getId(),
                getResult.getSeqNo(),
                getResult.getPrimaryTerm(),
                randomNonNegativeLong(),
                getResult.isExists(),
                getResult.internalSourceRef(),
                getResult.getFields(),
                null
            )
        );
        mutations.add(
            () -> new GetResult(
                getResult.getIndex(),
                getResult.getId(),
                getResult.isExists() ? UNASSIGNED_SEQ_NO : getResult.getSeqNo(),
                getResult.isExists() ? 0 : getResult.getPrimaryTerm(),
                getResult.getVersion(),
                getResult.isExists() == false,
                getResult.internalSourceRef(),
                getResult.getFields(),
                null
            )
        );
        mutations.add(
            () -> new GetResult(
                getResult.getIndex(),
                getResult.getId(),
                getResult.getSeqNo(),
                getResult.getPrimaryTerm(),
                getResult.getVersion(),
                getResult.isExists(),
                RandomObjects.randomSource(random()),
                getResult.getFields(),
                null
            )
        );
        mutations.add(
            () -> new GetResult(
                getResult.getIndex(),
                getResult.getId(),
                getResult.getSeqNo(),
                getResult.getPrimaryTerm(),
                getResult.getVersion(),
                getResult.isExists(),
                getResult.internalSourceRef(),
                randomDocumentFields(XContentType.JSON, randomBoolean()).v1(),
                null
            )
        );
        return randomFrom(mutations).get();
    }

    public static Tuple<GetResult, GetResult> randomGetResult(XContentType xContentType) {
        final String index = randomAlphaOfLengthBetween(3, 10);
        final String id = randomAlphaOfLengthBetween(3, 10);
        final long version;
        final long seqNo;
        final long primaryTerm;
        final boolean exists;
        BytesReference source = null;
        Map<String, DocumentField> docFields = null;
        Map<String, DocumentField> expectedDocFields = null;
        Map<String, DocumentField> metaFields = null;
        Map<String, DocumentField> expectedMetaFields = null;
        if (frequently()) {
            version = randomNonNegativeLong();
            seqNo = randomNonNegativeLong();
            primaryTerm = randomLongBetween(1, 100);
            exists = true;
            if (frequently()) {
                source = RandomObjects.randomSource(random());
            }
            if (randomBoolean()) {
                Tuple<Map<String, DocumentField>, Map<String, DocumentField>> tuple = randomDocumentFields(xContentType, false);
                docFields = tuple.v1();
                expectedDocFields = tuple.v2();

                tuple = randomDocumentFields(xContentType, true);
                metaFields = tuple.v1();
                expectedMetaFields = tuple.v2();
            }
        } else {
            seqNo = UNASSIGNED_SEQ_NO;
            primaryTerm = UNASSIGNED_PRIMARY_TERM;
            version = -1;
            exists = false;
        }
        GetResult getResult = new GetResult(index, id, seqNo, primaryTerm, version, exists, source, docFields, metaFields);
        GetResult expectedGetResult = new GetResult(
            index,
            id,
            seqNo,
            primaryTerm,
            version,
            exists,
            source,
            expectedDocFields,
            expectedMetaFields
        );
        return Tuple.tuple(getResult, expectedGetResult);
    }

    public static Tuple<Map<String, DocumentField>, Map<String, DocumentField>> randomDocumentFields(
        XContentType xContentType,
        boolean isMetaFields
    ) {
        int numFields = isMetaFields ? randomIntBetween(1, 3) : randomIntBetween(2, 10);
        Map<String, DocumentField> fields = Maps.newMapWithExpectedSize(numFields);
        Map<String, DocumentField> expectedFields = Maps.newMapWithExpectedSize(numFields);
        // As we are using this to construct a GetResult object that already contains
        // index, id, version, seqNo, and source fields, we need to exclude them from random fields
        Predicate<String> excludeMetaFieldFilter = field -> field.equals(IndexFieldMapper.NAME)
            || field.equals(IdFieldMapper.NAME)
            || field.equals(VersionFieldMapper.NAME)
            || field.equals(SourceFieldMapper.NAME)
            || field.equals(SeqNoFieldMapper.NAME);
        while (fields.size() < numFields) {
            Tuple<DocumentField, DocumentField> tuple = randomDocumentField(xContentType, isMetaFields, excludeMetaFieldFilter);
            DocumentField getField = tuple.v1();
            DocumentField expectedGetField = tuple.v2();
            if (fields.putIfAbsent(getField.getName(), getField) == null) {
                assertNull(expectedFields.putIfAbsent(expectedGetField.getName(), expectedGetField));
            }
        }
        return Tuple.tuple(fields, expectedFields);
    }

    private static BytesReference toXContentEmbedded(GetResult getResult, XContentType xContentType, boolean humanReadable)
        throws IOException {
        return XContentHelper.toXContent(getResult::toXContentEmbedded, xContentType, humanReadable);
    }
}
