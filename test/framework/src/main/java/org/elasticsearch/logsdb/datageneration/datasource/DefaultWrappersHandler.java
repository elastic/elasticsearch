/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import org.elasticsearch.test.ESTestCase;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class DefaultWrappersHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.NullWrapper handle(DataSourceRequest.NullWrapper ignored) {
        return new DataSourceResponse.NullWrapper(injectNulls());
    }

    @Override
    public DataSourceResponse.ArrayWrapper handle(DataSourceRequest.ArrayWrapper ignored) {
        return new DataSourceResponse.ArrayWrapper(wrapInArray());
    }

    private static Function<Supplier<Object>, Supplier<Object>> injectNulls() {
        return (values) -> () -> ESTestCase.randomBoolean() ? null : values.get();
    }

    private static Function<Supplier<Object>, Supplier<Object>> wrapInArray() {
        return (values) -> () -> {
            if (ESTestCase.randomBoolean()) {
                var size = ESTestCase.randomIntBetween(0, 5);
                return IntStream.range(0, size).mapToObj((i) -> values.get()).toList();
            }

            return values.get();
        };
    }
}
