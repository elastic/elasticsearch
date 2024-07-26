/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.logsdb.datageneration.arbitrary.Arbitrary;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class FieldValues {
    private FieldValues() {}

    public static Function<Supplier<Object>, Supplier<Object>> injectNulls(Arbitrary arbitrary) {
        return (values) -> () -> arbitrary.generateNullValue() ? null : values.get();
    }

    public static Function<Supplier<Object>, Supplier<Object>> wrappedInArray(Arbitrary arbitrary) {
        return (values) -> () -> {
            if (arbitrary.generateArrayOfValues()) {
                var size = arbitrary.valueArraySize();
                return IntStream.range(0, size).mapToObj((i) -> values.get()).toList();
            }

            return values.get();
        };
    }
}
