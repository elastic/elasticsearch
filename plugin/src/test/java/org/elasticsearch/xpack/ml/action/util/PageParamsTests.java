/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action.util;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class PageParamsTests extends AbstractSerializingTestCase<PageParams> {

    @Override
    protected PageParams doParseInstance(XContentParser parser) {
        return PageParams.PARSER.apply(parser, null);
    }

    @Override
    protected PageParams createTestInstance() {
        int from = randomInt(PageParams.MAX_FROM_SIZE_SUM);
        int maxSize = PageParams.MAX_FROM_SIZE_SUM - from;
        int size = randomInt(maxSize);
        return new PageParams(from, size);
    }

    @Override
    protected Reader<PageParams> instanceReader() {
        return PageParams::new;
    }

    public void testValidate_GivenFromIsMinusOne() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new PageParams(-1, 100));
        assertEquals("Parameter [from] cannot be < 0", e.getMessage());
    }

    public void testValidate_GivenFromIsMinusTen() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new PageParams(-10, 100));
        assertEquals("Parameter [from] cannot be < 0", e.getMessage());
    }

    public void testValidate_GivenSizeIsMinusOne() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new PageParams(0, -1));
        assertEquals("Parameter [size] cannot be < 0", e.getMessage());
    }

    public void testValidate_GivenSizeIsMinusHundred() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new PageParams(0, -100));
        assertEquals("Parameter [size] cannot be < 0", e.getMessage());
    }

    public void testValidate_GivenFromAndSizeSumIsMoreThan10000() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new PageParams(0, 10001));
        assertEquals("The sum of parameters [from] and [size] cannot be higher than 10000.", e.getMessage());
    }

    @Override
    protected PageParams mutateInstance(PageParams instance) throws IOException {
        int from = instance.getFrom();
        int size = instance.getSize();
        switch (between(0, 1)) {
        case 0:
            from += between(1, 20);
            // If we have gone above the limit for max and size then we need to
            // change size too
            if ((from + size) > PageParams.MAX_FROM_SIZE_SUM) {
                size = PageParams.MAX_FROM_SIZE_SUM - from;
            }
            break;
        case 1:
            size += between(1, 20);
            // If we have gone above the limit for max and size then we need to
            // change from too
            if ((from + size) > PageParams.MAX_FROM_SIZE_SUM) {
                from = PageParams.MAX_FROM_SIZE_SUM - size;
            }
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new PageParams(from, size);
    }
}
