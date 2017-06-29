/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collector;

import static java.util.stream.Collectors.toMap;

public abstract class ObjectUtils {

    private static final Consumer<Object> NO_OP_CONSUMER = whatever -> {};
    private static final Runnable NO_OP_RUNNER = () -> {};  
    
    public static <T> boolean isEmpty(Object[] array) {
        return (array == null || array.length == 0);
    }
    
    public static <T> Predicate<T> not(Predicate<T> predicate) {
        return predicate.negate();
    }

    @SuppressWarnings("unchecked")
    public static <T> Consumer<T> noOpConsumer() {
        return (Consumer<T>) NO_OP_CONSUMER;
    }

    public static Runnable noOpRunnable() {
        return NO_OP_RUNNER;
    }

    public static <K, V> Collector<Entry<K, V>, ?, LinkedHashMap<K, V>> mapCollector() {
        return toMap(
                Entry::getKey,
                Entry::getValue,
                (k1,k2) -> { throw new IllegalStateException("Duplicate key"); },
                LinkedHashMap::new);
    }
}
