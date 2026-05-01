/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.InferenceStringTests.TEST_DATA_URI;
import static org.elasticsearch.inference.RerankRequest.SUPPORTED_RERANK_DATA_TYPES;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RerankRequestTests extends AbstractBWCSerializationTestCase<RerankRequest> {

    public static final String INPUT_TEXT = "some text input";
    public static final String QUERY_TEXT = "some query";

    public void testParser_WithSingleStringInputAndQuery() throws IOException {
        var requestJson = Strings.format("""
            {
                "input": "%s",
                "query": "%s"
            }
            """, INPUT_TEXT, QUERY_TEXT);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = RerankRequest.PARSER.apply(parser, null);
            assertThat(request.inputs(), is(List.of(new InferenceString(DataType.TEXT, DataFormat.TEXT, INPUT_TEXT))));
            assertThat(request.query(), is(new InferenceString(DataType.TEXT, DataFormat.TEXT, QUERY_TEXT)));
            assertThat(request.topN(), is(nullValue()));
            assertThat(request.returnDocuments(), is(nullValue()));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_WithObjectInputAndQuery() throws IOException {
        var requestJson = Strings.format("""
            {
                "input": {"type":"text", "format":"text", "value":"%s"},
                "query": {"type":"text", "format":"text", "value":"%s"}
            }
            """, INPUT_TEXT, QUERY_TEXT);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = RerankRequest.PARSER.apply(parser, null);
            assertThat(request.inputs(), is(List.of(new InferenceString(DataType.TEXT, DataFormat.TEXT, INPUT_TEXT))));
            assertThat(request.query(), is(new InferenceString(DataType.TEXT, DataFormat.TEXT, QUERY_TEXT)));
            assertThat(request.topN(), is(nullValue()));
            assertThat(request.returnDocuments(), is(nullValue()));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_WithStringArrayInput() throws IOException {
        var input2 = "another text input";
        var requestJson = Strings.format("""
            {
                "input": ["%s","%s"],
                "query": "%s"
            }
            """, INPUT_TEXT, input2, QUERY_TEXT);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = RerankRequest.PARSER.apply(parser, null);
            assertThat(
                request.inputs(),
                is(
                    List.of(
                        new InferenceString(DataType.TEXT, DataFormat.TEXT, INPUT_TEXT),
                        new InferenceString(DataType.TEXT, DataFormat.TEXT, input2)
                    )
                )
            );
            assertThat(request.query(), is(new InferenceString(DataType.TEXT, DataFormat.TEXT, QUERY_TEXT)));
            assertThat(request.topN(), is(nullValue()));
            assertThat(request.returnDocuments(), is(nullValue()));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_WithObjectArrayInput() throws IOException {
        var firstInput = INPUT_TEXT + "1";
        var secondInput = INPUT_TEXT + "2";
        var requestJson = Strings.format("""
            {
                "input": [
                  {"type":"text", "format":"text", "value":"%s"},
                  {"type":"text", "format":"text", "value":"%s"}
                ],
                "query": {"type":"text", "format":"text", "value":"%s"}
            }
            """, firstInput, secondInput, QUERY_TEXT);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = RerankRequest.PARSER.apply(parser, null);
            assertThat(
                request.inputs(),
                is(
                    List.of(
                        new InferenceString(DataType.TEXT, DataFormat.TEXT, firstInput),
                        new InferenceString(DataType.TEXT, DataFormat.TEXT, secondInput)
                    )
                )
            );
            assertThat(request.query(), is(new InferenceString(DataType.TEXT, DataFormat.TEXT, QUERY_TEXT)));
            assertThat(request.topN(), is(nullValue()));
            assertThat(request.returnDocuments(), is(nullValue()));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_WithUnspecifiedFormats_UsesDefaults() throws IOException {
        var requestJson = Strings.format("""
            {
                "input": {"type":"text", "value":"%s"},
                "query": {"type":"text", "value":"%s"}
            }
            """, INPUT_TEXT, QUERY_TEXT);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = RerankRequest.PARSER.apply(parser, null);
            assertThat(request.inputs(), is(List.of(new InferenceString(DataType.TEXT, DataFormat.TEXT, INPUT_TEXT))));
            assertThat(request.query(), is(new InferenceString(DataType.TEXT, DataFormat.TEXT, QUERY_TEXT)));
            assertThat(request.topN(), is(nullValue()));
            assertThat(request.returnDocuments(), is(nullValue()));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_WithAllFieldsSet() throws IOException {
        var topN = randomIntBetween(1, 1028);
        var returnDocuments = randomBoolean();
        var fieldOne = "field_one";
        var valueOne = "value_one";
        var fieldTwo = "field_two";
        var valueTwo = 123;
        var requestJson = Strings.format("""
            {
                "input": "%s",
                "query": "%s",
                "top_n": %d,
                "return_documents": %b,
                "task_settings": {
                  "%s": "%s",
                  "%s": %d
                }
            }
            """, INPUT_TEXT, QUERY_TEXT, topN, returnDocuments, fieldOne, valueOne, fieldTwo, valueTwo);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = RerankRequest.PARSER.apply(parser, null);
            assertThat(request.inputs(), is(List.of(new InferenceString(DataType.TEXT, DataFormat.TEXT, INPUT_TEXT))));
            assertThat(request.query(), is(new InferenceString(DataType.TEXT, DataFormat.TEXT, QUERY_TEXT)));
            assertThat(request.topN(), is(topN));
            assertThat(request.returnDocuments(), is(returnDocuments));
            assertThat(request.taskSettings(), is(Map.of(fieldOne, valueOne, fieldTwo, valueTwo)));
        }
    }

    public void testParser_WithEmptyTaskSettings() throws IOException {
        var requestJson = Strings.format("""
            {
                "input": "%s",
                "query": "%s",
                "task_settings": {}
            }
            """, INPUT_TEXT, QUERY_TEXT);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = RerankRequest.PARSER.apply(parser, null);
            assertThat(request.inputs(), is(List.of(new InferenceString(DataType.TEXT, DataFormat.TEXT, INPUT_TEXT))));
            assertThat(request.query(), is(new InferenceString(DataType.TEXT, DataFormat.TEXT, QUERY_TEXT)));
            assertThat(request.topN(), is(nullValue()));
            assertThat(request.returnDocuments(), is(nullValue()));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_WithUnsupportedRerankDataTypeInInputs_Throws() throws IOException {
        var unsupportedDataType = randomFrom(EnumSet.complementOf(SUPPORTED_RERANK_DATA_TYPES));
        var requestJson = Strings.format("""
            {
                "input": {"type":"%s", "value":"%s"},
                "query": "%s"
            }
            """, unsupportedDataType, TEST_DATA_URI, QUERY_TEXT);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = expectThrows(XContentParseException.class, () -> RerankRequest.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), containsString("[RerankRequest] failed to parse field [input]"));
            assertThat(
                exception.getCause().getMessage(),
                containsString(
                    Strings.format("Field [input] contains unsupported [type] value [%s]. Supported values are [text]", unsupportedDataType)
                )
            );
        }
    }

    public void testParser_WithUnsupportedRerankDataTypeInQuery_Throws() throws IOException {
        var unsupportedDataType = randomFrom(EnumSet.complementOf(SUPPORTED_RERANK_DATA_TYPES));
        var requestJson = Strings.format("""
            {
                "input": "%s",
                "query": {"type":"%s", "value":"%s"}
            }
            """, INPUT_TEXT, unsupportedDataType, TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = expectThrows(XContentParseException.class, () -> RerankRequest.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), containsString("[RerankRequest] failed to parse field [query]"));
            assertThat(
                exception.getCause().getMessage(),
                containsString(
                    Strings.format("Field [query] contains unsupported [type] value [%s]. Supported values are [text]", unsupportedDataType)
                )
            );
        }
    }

    @Override
    protected Writeable.Reader<RerankRequest> instanceReader() {
        return RerankRequest::new;
    }

    @Override
    protected RerankRequest createTestInstance() {
        return createRandom();
    }

    public static RerankRequest createRandom() {
        return new RerankRequest(
            randomInputs(),
            getRandomSupportedInferenceString(),
            randomFrom(randomIntBetween(1, 1028), null),
            randomOptionalBoolean(),
            Map.of(randomAlphanumericOfLength(8), randomAlphanumericOfLength(8))
        );
    }

    private static List<InferenceString> randomInputs() {
        var contents = new ArrayList<InferenceString>();
        for (int i = 0; i < randomIntBetween(1, 5); ++i) {
            contents.add(getRandomSupportedInferenceString());
        }
        return contents;
    }

    public static InferenceString getRandomSupportedInferenceString() {
        return InferenceStringTests.createRandomUsingDataTypes(SUPPORTED_RERANK_DATA_TYPES);
    }

    @Override
    protected RerankRequest mutateInstance(RerankRequest instance) throws IOException {
        var inputs = instance.inputs();
        var query = instance.query();
        var topN = instance.topN();
        var returnDocuments = instance.returnDocuments();
        var taskSettings = instance.taskSettings();
        switch (randomInt(4)) {
            case 0 -> inputs = randomValueOtherThan(inputs, RerankRequestTests::randomInputs);
            case 1 -> query = randomValueOtherThan(query, RerankRequestTests::getRandomSupportedInferenceString);
            case 2 -> topN = randomValueOtherThan(topN, () -> randomFrom(randomIntBetween(1, 1028), null));
            case 3 -> returnDocuments = randomValueOtherThan(returnDocuments, ESTestCase::randomOptionalBoolean);
            case 4 -> taskSettings = randomValueOtherThan(
                taskSettings,
                () -> Map.of(randomAlphanumericOfLength(8), randomAlphanumericOfLength(8))
            );
        }
        return new RerankRequest(inputs, query, topN, returnDocuments, taskSettings);
    }

    @Override
    protected RerankRequest mutateInstanceForVersion(RerankRequest instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected RerankRequest doParseInstance(XContentParser parser) throws IOException {
        return RerankRequest.PARSER.parse(parser, null);
    }
}
