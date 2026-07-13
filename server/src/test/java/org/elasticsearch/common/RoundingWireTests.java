/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.common.Rounding.DateTimeUnit;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.concurrent.TimeUnit;

public class RoundingWireTests extends AbstractWireSerializingTestCase<Rounding> {
    @Override
    protected Rounding createTestInstance() {
        Rounding.Builder builder;
        if (randomBoolean()) {
            builder = Rounding.builder(randomFrom(DateTimeUnit.values()));
        } else {
            // The time value's millisecond component must be > 0 so we're limited in the suffixes we can use.
            builder = Rounding.builder(
                randomTimeValue(1, 1000, TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.MINUTES, TimeUnit.SECONDS, TimeUnit.MILLISECONDS)
            );
        }
        if (randomBoolean()) {
            builder.timeZone(randomZone());
        }
        if (randomBoolean()) {
            builder.offset(randomLong());
        }
        return builder.build();
    }

    @Override
    protected Rounding mutateInstance(Rounding instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<Rounding> instanceReader() {
        return Rounding::read;
    }
}
