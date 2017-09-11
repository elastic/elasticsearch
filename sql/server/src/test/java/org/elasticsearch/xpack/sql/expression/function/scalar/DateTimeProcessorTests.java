/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeExtractor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;

public class DateTimeProcessorTests extends AbstractWireSerializingTestCase<DateTimeProcessor> {
    public static DateTimeProcessor randomDateTimeProcessor() {
        return new DateTimeProcessor(randomFrom(DateTimeExtractor.values()), DateTimeZone.UTC);
    }

    @Override
    protected DateTimeProcessor createTestInstance() {
        return randomDateTimeProcessor();
    }

    @Override
    protected Reader<DateTimeProcessor> instanceReader() {
        return DateTimeProcessor::new;
    }

    @Override
    protected DateTimeProcessor mutateInstance(DateTimeProcessor instance) throws IOException {
        return new DateTimeProcessor(randomValueOtherThan(instance.extractor(), () -> randomFrom(DateTimeExtractor.values())),
                DateTimeZone.UTC);
    }

    public void testApply() {
        DateTimeProcessor proc = new DateTimeProcessor(DateTimeExtractor.YEAR, DateTimeZone.UTC);
        assertEquals(1970, proc.apply(0L));
        assertEquals(1970, proc.apply(new DateTime(0L, DateTimeZone.UTC)));
        assertEquals(2017, proc.apply(new DateTime(2017, 01, 02, 10, 10, DateTimeZone.UTC)));

        proc = new DateTimeProcessor(DateTimeExtractor.DAY_OF_MONTH, DateTimeZone.UTC);
        assertEquals(1, proc.apply(0L));
        assertEquals(1, proc.apply(new DateTime(0L, DateTimeZone.UTC)));
        assertEquals(2, proc.apply(new DateTime(2017, 01, 02, 10, 10, DateTimeZone.UTC)));
        assertEquals(31, proc.apply(new DateTime(2017, 01, 31, 10, 10, DateTimeZone.UTC)));
    }
}
