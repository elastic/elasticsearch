/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Date;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class QuantilesTests extends AbstractSerializingTestCase<Quantiles> {

    public void testExtractJobId_GivenValidDocId() {
        assertThat(Quantiles.extractJobId("foo_quantiles"), equalTo("foo"));
        assertThat(Quantiles.extractJobId("bar_quantiles"), equalTo("bar"));
        assertThat(Quantiles.extractJobId("foo_bar_quantiles"), equalTo("foo_bar"));
        assertThat(Quantiles.extractJobId("_quantiles_quantiles"), equalTo("_quantiles"));
    }

    public void testExtractJobId_GivenV54DocId() {
        assertThat(Quantiles.extractJobId("foo-quantiles"), equalTo("foo"));
        assertThat(Quantiles.extractJobId("bar-quantiles"), equalTo("bar"));
        assertThat(Quantiles.extractJobId("foo-bar-quantiles"), equalTo("foo-bar"));
        assertThat(Quantiles.extractJobId("-quantiles-quantiles"), equalTo("-quantiles"));
    }

    public void testExtractJobId_GivenInvalidDocId() {
        assertThat(Quantiles.extractJobId(""), is(nullValue()));
        assertThat(Quantiles.extractJobId("foo"), is(nullValue()));
        assertThat(Quantiles.extractJobId("_quantiles"), is(nullValue()));
        assertThat(Quantiles.extractJobId("foo_model_state_3141341341"), is(nullValue()));
    }

    public void testEquals_GivenSameObject() {
        Quantiles quantiles = new Quantiles("foo", new Date(0L), "foo");
        assertTrue(quantiles.equals(quantiles));
    }


    public void testEquals_GivenDifferentClassObject() {
        Quantiles quantiles = new Quantiles("foo", new Date(0L), "foo");
        assertFalse(quantiles.equals("not a quantiles object"));
    }


    public void testEquals_GivenEqualQuantilesObject() {
        Quantiles quantiles1 = new Quantiles("foo", new Date(0L), "foo");

        Quantiles quantiles2 = new Quantiles("foo", new Date(0L), "foo");

        assertTrue(quantiles1.equals(quantiles2));
        assertTrue(quantiles2.equals(quantiles1));
    }


    public void testEquals_GivenDifferentState() {
        Quantiles quantiles1 = new Quantiles("foo", new Date(0L), "bar1");

        Quantiles quantiles2 = new Quantiles("foo", new Date(0L), "bar2");

        assertFalse(quantiles1.equals(quantiles2));
        assertFalse(quantiles2.equals(quantiles1));
    }


    public void testHashCode_GivenEqualObject() {
        Quantiles quantiles1 = new Quantiles("foo", new Date(0L), "foo");

        Quantiles quantiles2 = new Quantiles("foo", new Date(0L), "foo");

        assertEquals(quantiles1.hashCode(), quantiles2.hashCode());
    }

    public void testDocumentId() {
        Quantiles quantiles = createTestInstance();
        String jobId = quantiles.getJobId();
        assertEquals(jobId + "_quantiles", Quantiles.documentId(jobId));
    }

    @Override
    protected Quantiles createTestInstance() {
        return createRandomized();
    }

    public static Quantiles createRandomized() {
        return new Quantiles(randomAlphaOfLengthBetween(1, 20),
                new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()),
                randomAlphaOfLengthBetween(0, 1000));
    }

    @Override
    protected Reader<Quantiles> instanceReader() {
        return Quantiles::new;
    }

    @Override
    protected Quantiles doParseInstance(XContentParser parser) {
        return Quantiles.STRICT_PARSER.apply(parser, null);
    }

    public void testStrictParser() throws IOException {
        String json = "{\"job_id\":\"job_1\", \"timestamp\": 123456789, \"quantile_state\":\"...\", \"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> Quantiles.STRICT_PARSER.apply(parser, null));

            assertThat(e.getMessage(), containsString("unknown field [foo]"));
        }
    }

    public void testLenientParser() throws IOException {
        String json = "{\"job_id\":\"job_1\", \"timestamp\": 123456789, \"quantile_state\":\"...\", \"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            Quantiles.LENIENT_PARSER.apply(parser, null);
        }
    }
}
