/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action.util;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

public class PageParamsTests extends AbstractXContentSerializingTestCase<PageParams> {

    @Override
    protected PageParams doParseInstance(XContentParser parser) {
        return PageParams.PARSER.apply(parser, null);
    }

    @Override
    protected PageParams createTestInstance() {
        int from = randomInt(10000);
        int size = randomInt(10000);
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

    @Override
    protected PageParams mutateInstance(PageParams instance) {
        int from = instance.getFrom();
        int size = instance.getSize();
        int amountToAdd = between(1, 20);
        switch (between(0, 1)) {
            case 0 -> from += amountToAdd;
            case 1 -> size += amountToAdd;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PageParams(from, size);
    }
}
