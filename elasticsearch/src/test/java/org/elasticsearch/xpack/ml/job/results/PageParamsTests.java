/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

public class PageParamsTests extends AbstractSerializingTestCase<PageParams> {

    @Override
    protected PageParams parseInstance(XContentParser parser) {
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
}
