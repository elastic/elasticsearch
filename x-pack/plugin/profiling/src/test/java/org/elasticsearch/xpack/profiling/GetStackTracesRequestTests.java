/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;

public class GetStackTracesRequestTests extends ESTestCase {
    public void testSerialization() throws IOException {
        Integer sampleSize = randomIntBetween(1, Integer.MAX_VALUE);
        Double requestedDuration = randomBoolean() ? randomDoubleBetween(0.001d, Double.MAX_VALUE, true) : null;
        Double customCostFactor = randomBoolean() ? randomDoubleBetween(0.1d, 5.0d, true) : null;
        QueryBuilder query = randomBoolean() ? new BoolQueryBuilder() : null;

        GetStackTracesRequest request = new GetStackTracesRequest(
            sampleSize,
            requestedDuration,
            customCostFactor,
            query,
            null,
            null,
            null,
            null,
            null
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                GetStackTracesRequest deserialized = new GetStackTracesRequest(in);
                assertEquals(sampleSize, deserialized.getSampleSize());
                assertEquals(query, deserialized.getQuery());
            }
        }
    }

    public void testParseValidXContent() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
            .startObject()
                .field("sample_size", 500)
                .field("requested_duration", 100.54d)
                .startObject("query")
                    .startObject("range")
                        .startObject("@timestamp")
                            .field("gte", "2022-10-05")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
        //end::noformat
        )) {

            GetStackTracesRequest request = new GetStackTracesRequest();
            request.parseXContent(content);

            assertEquals(Integer.valueOf(500), request.getSampleSize());
            assertEquals(Double.valueOf(100.54d), request.getRequestedDuration());
            // a basic check suffices here
            assertEquals("@timestamp", ((RangeQueryBuilder) request.getQuery()).fieldName());
        }
    }

    public void testParseValidXContentWithCustomIndex() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
            .startObject()
                .field("sample_size", 2000)
                .field("indices", "my-traces")
                .field("stacktrace_ids", "stacktraces")
                .startObject("query")
                    .startObject("range")
                        .startObject("@timestamp")
                            .field("gte", "2022-10-05")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
        //end::noformat
        )) {

            GetStackTracesRequest request = new GetStackTracesRequest();
            request.parseXContent(content);

            assertEquals(Integer.valueOf(2000), request.getSampleSize());
            assertEquals("my-traces", request.getIndices());
            assertEquals("stacktraces", request.getStackTraceIds());
            // a basic check suffices here
            assertEquals("@timestamp", ((RangeQueryBuilder) request.getQuery()).fieldName());

            // Expect the default values
            assertEquals(null, request.getRequestedDuration());
        }
    }

    public void testParseXContentUnrecognizedField() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
            .startObject()
                // should be sample_size
                .field("sample-size", 500)
                .startObject("query")
                    .startObject("range")
                        .startObject("@timestamp")
                            .field("gte", "2022-10-05")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
        //end::noformat
        )) {

            GetStackTracesRequest request = new GetStackTracesRequest();
            ParsingException ex = expectThrows(ParsingException.class, () -> request.parseXContent(content));
            assertEquals("Unknown key for a VALUE_NUMBER in [sample-size].", ex.getMessage());
        }
    }

    public void testValidateWrongSampleSize() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            randomIntBetween(Integer.MIN_VALUE, 0),
            1.0d,
            1.0d,
            null,
            null,
            null,
            null,
            null,
            null
        );
        List<String> validationErrors = request.validate().validationErrors();
        assertEquals(1, validationErrors.size());
        assertTrue(validationErrors.get(0).contains("[sample_size] must be greater than 0,"));
    }

    public void testValidateStacktraceWithoutIndices() {
        GetStackTracesRequest request = new GetStackTracesRequest(1, 1.0d, 1.0d, null, null, randomAlphaOfLength(3), null, null, null);
        List<String> validationErrors = request.validate().validationErrors();
        assertEquals(1, validationErrors.size());
        assertEquals("[stacktrace_ids] must not be set", validationErrors.get(0));
    }

    public void testValidateIndicesWithoutStacktraces() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            null,
            1.0d,
            1.0d,
            null,
            randomAlphaOfLength(5),
            randomFrom("", null),
            null,
            null,
            null
        );
        List<String> validationErrors = request.validate().validationErrors();
        assertEquals(1, validationErrors.size());
        assertEquals("[stacktrace_ids] is mandatory", validationErrors.get(0));
    }

    public void testConsidersCustomIndicesInRelatedIndices() {
        String customIndex = randomAlphaOfLength(5);
        GetStackTracesRequest request = new GetStackTracesRequest(
            1,
            1.0d,
            1.0d,
            null,
            customIndex,
            randomAlphaOfLength(3),
            null,
            null,
            null
        );
        String[] indices = request.indices();
        assertEquals(4, indices.length);
        assertTrue("custom index not contained in indices list", Set.of(indices).contains(customIndex));
    }

    public void testConsidersDefaultIndicesInRelatedIndices() {
        String customIndex = randomAlphaOfLength(5);
        GetStackTracesRequest request = new GetStackTracesRequest(1, 1.0d, 1.0d, null, null, null, null, null, null);
        String[] indices = request.indices();
        assertEquals(15, indices.length);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        // to register the query parser
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents());
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedWriteables());
    }
}
