/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.test.ESTestCase;

public class DoubleDateTransformerTests extends ESTestCase {

    public void testTransform_GivenTimestampIsNotMilliseconds() throws CannotParseTimestampException {
        DoubleDateTransformer transformer = new DoubleDateTransformer(false);

        assertEquals(1000000, transformer.transform("1000"));
    }

    public void testTransform_GivenTimestampIsMilliseconds() throws CannotParseTimestampException {
        DoubleDateTransformer transformer = new DoubleDateTransformer(true);

        assertEquals(1000, transformer.transform("1000"));
    }

    public void testTransform_GivenTimestampIsNotValidDouble() throws CannotParseTimestampException {
        DoubleDateTransformer transformer = new DoubleDateTransformer(false);

        CannotParseTimestampException e = ESTestCase.expectThrows(CannotParseTimestampException.class,
                () -> transformer.transform("invalid"));
        assertEquals("Cannot parse timestamp 'invalid' as epoch value", e.getMessage());
    }
}
