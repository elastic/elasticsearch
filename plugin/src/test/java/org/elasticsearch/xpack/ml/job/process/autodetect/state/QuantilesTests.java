/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.util.Date;

public class QuantilesTests extends AbstractSerializingTestCase<Quantiles> {

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
        return Quantiles.PARSER.apply(parser, null);
    }
}
