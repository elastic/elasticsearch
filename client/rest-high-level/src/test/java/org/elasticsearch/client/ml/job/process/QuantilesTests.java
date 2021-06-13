/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.Date;


public class QuantilesTests extends AbstractXContentTestCase<Quantiles> {

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
    protected Quantiles doParseInstance(XContentParser parser) {
        return Quantiles.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
