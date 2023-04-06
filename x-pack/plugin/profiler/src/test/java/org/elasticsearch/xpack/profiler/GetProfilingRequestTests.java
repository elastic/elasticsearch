/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

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

import static java.util.Collections.emptyList;

public class GetProfilingRequestTests extends ESTestCase {
    public void testSerialization() throws IOException {
        Integer sampleSize = randomBoolean() ? randomIntBetween(0, Integer.MAX_VALUE) : null;
        QueryBuilder query = randomBoolean() ? new BoolQueryBuilder() : null;

        GetProfilingRequest request = new GetProfilingRequest(sampleSize, query);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                GetProfilingRequest deserialized = new GetProfilingRequest(in);
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

            GetProfilingRequest profilingRequest = new GetProfilingRequest();
            profilingRequest.parseXContent(content);

            assertEquals(Integer.valueOf(500), profilingRequest.getSampleSize());
            // a basic check suffices here
            assertEquals("@timestamp", ((RangeQueryBuilder) profilingRequest.getQuery()).fieldName());
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

            GetProfilingRequest profilingRequest = new GetProfilingRequest();
            ParsingException ex = expectThrows(ParsingException.class, () -> profilingRequest.parseXContent(content));
            assertEquals("Unknown key for a VALUE_NUMBER in [sample-size].", ex.getMessage());
        }
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
