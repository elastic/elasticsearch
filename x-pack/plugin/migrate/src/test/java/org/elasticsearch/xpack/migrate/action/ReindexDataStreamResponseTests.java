/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction.ReindexDataStreamResponse;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class ReindexDataStreamResponseTests extends AbstractWireSerializingTestCase<ReindexDataStreamResponse> {
    @Override
    protected Writeable.Reader<ReindexDataStreamResponse> instanceReader() {
        return ReindexDataStreamResponse::new;
    }

    @Override
    protected ReindexDataStreamResponse createTestInstance() {
        return new ReindexDataStreamResponse(randomAlphaOfLength(40));
    }

    @Override
    protected ReindexDataStreamResponse mutateInstance(ReindexDataStreamResponse instance) {
        return createTestInstance();
    }

    public void testToXContent() throws IOException {
        ReindexDataStreamResponse response = createTestInstance();
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            builder.humanReadable(true);
            response.toXContent(builder, EMPTY_PARAMS);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                assertThat(parser.map(), equalTo(Map.of("task", response.getTaskId())));
            }
        }
    }
}
