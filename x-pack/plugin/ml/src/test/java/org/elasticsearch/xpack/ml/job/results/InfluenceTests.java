/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.job.results.Influence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class InfluenceTests extends AbstractXContentSerializingTestCase<Influence> {

    @Override
    protected Influence createTestInstance() {
        int size = randomInt(10);
        List<String> fieldValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            fieldValues.add(randomAlphaOfLengthBetween(1, 20));
        }
        return new Influence(randomAlphaOfLengthBetween(1, 30), fieldValues);
    }

    @Override
    protected Influence mutateInstance(Influence instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<Influence> instanceReader() {
        return Influence::new;
    }

    @Override
    protected Influence doParseInstance(XContentParser parser) {
        return Influence.STRICT_PARSER.apply(parser, null);
    }

    public void testStrictParser() throws IOException {
        String json = """
            {"influencer_field_name":"influencer_1", "influencer_field_values":[], "foo":"bar"}
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Influence.STRICT_PARSER.apply(parser, null));

            assertThat(e.getMessage(), containsString("unknown field [foo]"));
        }
    }

    public void testLenientParser() throws IOException {
        String json = """
            {"influencer_field_name":"influencer_1", "influencer_field_values":[], "foo":"bar"}
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            Influence.LENIENT_PARSER.apply(parser, null);
        }
    }
}
