/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
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

    @Override
    public DataSourceResponse.RepeatingWrapper handle(DataSourceRequest.RepeatingWrapper ignored) {
        return new DataSourceResponse.RepeatingWrapper(repeatValues());
    }

    @Override
    public DataSourceResponse.MalformedWrapper handle(DataSourceRequest.MalformedWrapper request) {
        return new DataSourceResponse.MalformedWrapper(injectMalformed(request.malformedValues()));
    }

    @Override
    public DataSourceResponse.TransformWrapper handle(DataSourceRequest.TransformWrapper request) {
        return new DataSourceResponse.TransformWrapper(transform(request.transformedProportion(), request.transformation()));
    }

    private static Function<Supplier<Object>, Supplier<Object>> injectNulls() {
        // Inject some nulls but majority of data should be non-null (as it likely is in reality).
        return transform(0.05, ignored -> null);
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

    private static Function<Supplier<Object>, Supplier<Object>> repeatValues() {
        return (values) -> {
            HashSet<Object> previousValues = new HashSet<>();
            return () -> {
                if (previousValues.size() > 0 && ESTestCase.randomBoolean()) {
                    return ESTestCase.randomFrom(previousValues);
                } else {
                    var value = values.get();
                    previousValues.add(value);
                    return value;
                }
            };
        };
    }

    private static Function<Supplier<Object>, Supplier<Object>> injectMalformed(Supplier<Object> malformedValues) {
        return transform(0.1, ignored -> malformedValues.get());
    }

    private static Function<Supplier<Object>, Supplier<Object>> transform(
        double transformedProportion,
        Function<Object, Object> transformation
    ) {
        return (values) -> () -> ESTestCase.randomDouble() <= transformedProportion ? transformation.apply(values.get()) : values.get();
    }
}
