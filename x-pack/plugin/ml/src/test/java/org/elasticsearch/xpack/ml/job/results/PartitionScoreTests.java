/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.results.PartitionScore;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class PartitionScoreTests extends AbstractSerializingTestCase<PartitionScore> {

    @Override
    protected PartitionScore createTestInstance() {
        return new PartitionScore(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20), randomDouble(), randomDouble(),
                randomDouble());
    }

    @Override
    protected Reader<PartitionScore> instanceReader() {
        return PartitionScore::new;
    }

    @Override
    protected PartitionScore doParseInstance(XContentParser parser) {
        return PartitionScore.STRICT_PARSER.apply(parser, null);
    }

    public void testStrictParser() throws IOException {
        String json = "{\"partition_field_name\":\"field_1\", \"partition_field_value\":\"x\", \"initial_record_score\": 3," +
                " \"record_score\": 3, \"probability\": 0.001, \"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> PartitionScore.STRICT_PARSER.apply(parser, null));

            assertThat(e.getMessage(), containsString("unknown field [foo]"));
        }
    }

    public void testLenientParser() throws IOException {
        String json = "{\"partition_field_name\":\"field_1\", \"partition_field_value\":\"x\", \"initial_record_score\": 3," +
                " \"record_score\": 3, \"probability\": 0.001, \"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            PartitionScore.LENIENT_PARSER.apply(parser, null);
        }
    }
}
