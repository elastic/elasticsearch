/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.get;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.get.GetResultTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.index.get.GetResultTests.copyGetResult;
import static org.elasticsearch.index.get.GetResultTests.mutateGetResult;
import static org.elasticsearch.index.get.GetResultTests.randomGetResult;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class GetResponseTests extends ESTestCase {

    public void testToAndFromXContent() throws Exception {
        doFromXContentTestWithRandomFields(false);
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to
     * ensure we can parse it back to be forward compatible with additions to
     * the xContent
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<GetResult, GetResult> tuple = randomGetResult(xContentType);
        GetResponse getResponse = new GetResponse(tuple.v1());
        GetResponse expectedGetResponse = new GetResponse(tuple.v2());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(getResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable, "_source");

        BytesReference mutated;
        if (addRandomFields) {
            // "_source" and "fields" just consists of key/value pairs, we shouldn't add anything random there. It is already
            // randomized in the randomGetResult() method anyway. Also, we cannot add anything in the root object since this is
            // where GetResult's metadata fields are rendered out while // other fields are rendered out in a "fields" object.
            Predicate<String> excludeFilter = (s) -> s.isEmpty() || s.contains("fields") || s.contains("_source");
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        GetResponse parsedGetResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            parsedGetResponse = parseInstance(parser);
            assertNull(parser.nextToken());
        }
        assertEquals(expectedGetResponse.getSourceAsMap(), parsedGetResponse.getSourceAsMap());
        // print the parsed object out and test that the output is the same as the original output
        BytesReference finalBytes = toXContent(parsedGetResponse, xContentType, humanReadable);
        assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
        // check that the source stays unchanged, no shuffling of keys nor anything like that
        assertEquals(expectedGetResponse.getSourceAsString(), parsedGetResponse.getSourceAsString());
    }

    public void testToXContent() throws IOException {
        {
            GetResponse getResponse = new GetResponse(
                new GetResult(
                    "index",
                    "id",
                    0,
                    1,
                    1,
                    true,
                    new BytesArray("""
                        { "field1" : "value1", "field2":"value2"}"""),
                    Collections.singletonMap("field1", new DocumentField("field1", Collections.singletonList("value1"))),
                    null
                )
            );
            String output = Strings.toString(getResponse);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "_index": "index",
                  "_id": "id",
                  "_version": 1,
                  "_seq_no": 0,
                  "_primary_term": 1,
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
            GetResponse getResponse = new GetResponse(new GetResult("index", "id", UNASSIGNED_SEQ_NO, 0, 1, false, null, null, null));
            String output = Strings.toString(getResponse);
            assertEquals("""
                {"_index":"index","_id":"id","found":false}""", output);
        }
    }

    public void testToString() throws IOException {
        GetResponse getResponse = new GetResponse(
            new GetResult(
                "index",
                "id",
                0,
                1,
                1,
                true,
                new BytesArray("""
                    { "field1" : "value1", "field2":"value2"}"""),
                Collections.singletonMap("field1", new DocumentField("field1", Collections.singletonList("value1"))),
                null
            )
        );
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_index": "index",
              "_id": "id",
              "_version": 1,
              "_seq_no": 0,
              "_primary_term": 1,
              "found": true,
              "_source": {
                "field1": "value1",
                "field2": "value2"
              },
              "fields": {
                "field1": [ "value1" ]
              }
            }"""), XContentHelper.stripWhitespace(getResponse.toString()));
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(
            new GetResponse(randomGetResult(XContentType.JSON).v1()),
            GetResponseTests::copyGetResponse,
            GetResponseTests::mutateGetResponse
        );
    }

    public void testFromXContentThrowsParsingException() throws IOException {
        GetResponse getResponse = new GetResponse(
            new GetResult(null, null, UNASSIGNED_SEQ_NO, 0, randomIntBetween(1, 5), randomBoolean(), null, null, null)
        );

        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(getResponse, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());

        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInstance(parser));
            assertEquals("Missing required fields [_index,_id]", exception.getMessage());
        }
    }

    private static GetResponse copyGetResponse(GetResponse getResponse) {
        return new GetResponse(copyGetResult(getResponse.getResult));
    }

    private static GetResponse mutateGetResponse(GetResponse getResponse) {
        return new GetResponse(mutateGetResult(getResponse.getResult));
    }

    private static GetResponse parseInstance(XContentParser parser) throws IOException {
        GetResult getResult = GetResultTests.parseInstance(parser);

        // At this stage we ensure that we parsed enough information to return
        // a valid GetResponse instance. If it's not the case, we throw an
        // exception so that callers know it and can handle it correctly.
        if (getResult.getIndex() == null && getResult.getId() == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(Locale.ROOT, "Missing required fields [%s,%s]", GetResult._INDEX, GetResult._ID)
            );
        }
        return new GetResponse(getResult);
    }
}
