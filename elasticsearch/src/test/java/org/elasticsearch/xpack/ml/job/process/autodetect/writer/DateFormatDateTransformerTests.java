/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.test.ESTestCase;

public class DateFormatDateTransformerTests extends ESTestCase {

    public void testTransform_GivenValidTimestamp() throws CannotParseTimestampException {
        DateFormatDateTransformer transformer = new DateFormatDateTransformer("yyyy-MM-dd HH:mm:ssXXX");

        assertEquals(1388534400000L, transformer.transform("2014-01-01 00:00:00Z"));
    }

    public void testTransform_GivenInvalidTimestamp() throws CannotParseTimestampException {
        DateFormatDateTransformer transformer = new DateFormatDateTransformer("yyyy-MM-dd HH:mm:ssXXX");

        CannotParseTimestampException e = ESTestCase.expectThrows(CannotParseTimestampException.class,
                () -> transformer.transform("invalid"));
        assertEquals("Cannot parse date 'invalid' with format string 'yyyy-MM-dd HH:mm:ssXXX'", e.getMessage());
    }
}
