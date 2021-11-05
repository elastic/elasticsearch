/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class GeoResultsTests extends AbstractSerializingTestCase<GeoResults> {

    private boolean lenient;

    @Before
    public void setLenient() {
        lenient = randomBoolean();
    }

    static GeoResults createTestGeoResults() {
        GeoResults geoResults = new GeoResults();
        if (randomBoolean()) {
            geoResults.setActualPoint(randomDoubleBetween(-90.0, 90.0, true) + "," + randomDoubleBetween(-90.0, 90.0, true));
        }
        if (randomBoolean()) {
            geoResults.setTypicalPoint(randomDoubleBetween(-90.0, 90.0, true) + "," + randomDoubleBetween(-90.0, 90.0, true));
        }
        return geoResults;
    }

    @Override
    protected GeoResults createTestInstance() {
        return createTestGeoResults();
    }

    @Override
    protected Reader<GeoResults> instanceReader() {
        return GeoResults::new;
    }

    @Override
    protected GeoResults doParseInstance(XContentParser parser) {
        return lenient ? GeoResults.LENIENT_PARSER.apply(parser, null) : GeoResults.STRICT_PARSER.apply(parser, null);
    }

    public void testStrictParser() throws IOException {
        String json = "{\"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> GeoResults.STRICT_PARSER.apply(parser, null));

            assertThat(e.getMessage(), containsString("unknown field [foo]"));
        }
    }

    public void testLenientParser() throws IOException {
        String json = "{\"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            GeoResults.LENIENT_PARSER.apply(parser, null);
        }
    }
}
