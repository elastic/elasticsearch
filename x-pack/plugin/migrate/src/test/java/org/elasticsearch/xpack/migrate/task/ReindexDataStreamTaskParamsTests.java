/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class ReindexDataStreamTaskParamsTests extends AbstractXContentSerializingTestCase<ReindexDataStreamTaskParams> {

    @Override
    protected Writeable.Reader<ReindexDataStreamTaskParams> instanceReader() {
        return ReindexDataStreamTaskParams::new;
    }

    @Override
    protected ReindexDataStreamTaskParams createTestInstance() {
        return new ReindexDataStreamTaskParams(randomAlphaOfLength(50), randomLong(), randomNonNegativeInt(), randomNonNegativeInt());
    }

    @Override
    protected ReindexDataStreamTaskParams mutateInstance(ReindexDataStreamTaskParams instance) {
        String sourceDataStream = instance.sourceDataStream();
        long startTime = instance.startTime();
        int totalIndices = instance.totalIndices();
        int totalIndicesToBeUpgraded = instance.totalIndicesToBeUpgraded();
        switch (randomIntBetween(0, 3)) {
            case 0 -> sourceDataStream = randomAlphaOfLength(50);
            case 1 -> startTime = randomLong();
            case 2 -> totalIndices = totalIndices + 1;
            case 3 -> totalIndices = totalIndicesToBeUpgraded + 1;
            default -> throw new UnsupportedOperationException();
        }
        return new ReindexDataStreamTaskParams(sourceDataStream, startTime, totalIndices, totalIndicesToBeUpgraded);
    }

    @Override
    protected ReindexDataStreamTaskParams doParseInstance(XContentParser parser) {
        return ReindexDataStreamTaskParams.fromXContent(parser);
    }

    public void testToXContent() throws IOException {
        ReindexDataStreamTaskParams params = createTestInstance();
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            builder.humanReadable(true);
            params.toXContent(builder, EMPTY_PARAMS);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                Map<String, Object> parserMap = parser.map();
                assertThat(parserMap.get("source_data_stream"), equalTo(params.sourceDataStream()));
                assertThat(((Number) parserMap.get("start_time")).longValue(), equalTo(params.startTime()));
            }
        }
    }
}
