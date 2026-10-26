/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Utility class for wrapping collections in order to intercept mutating operations. Use it like
 * {@code reportingWritesUsing(someReporter).wrap(someMap, someLabel)}, where {@code someReporter} is a {@code Consumer<String>} which acts
 * as a callback which will be invoked when a mutating operation is performed with an argument describing the mutating operation,
 * {@code someMap} is the {@link Map} to wrap, and {@code someLabel} is a name to use for the map in the strings passed to the callback.
 *
 * <p>N.B. The map returned by the {@link #wrap} method has the same generic type parameters as its argument. It is
 * <strong>not safe</strong> to call this with generic type parameters which are subtypes of {@link Map}, {@link List}, or {@link Set}.
 * So, for example, it is okay to pass in a {@code Map<String, Map<String, List<String>>>}. But if you pass in a
 * {@code Map<String, Map<String, ArrayList<String>>>} and then do two levels of {@code get} calls on the result, you will get something
 * which is statically typed as an {@code ArrayList<String>} but its runtime type is a different {@link List} implementation, and you will
 * get a {@link ClassCastException} if you try to assign it to an {@code ArrayList}. None of this is a problem if you are using a JSON-like
 * static type of {@code Map<String, Object>} — unless you downcast an {@link Object} to a collection subtype such as {@link ArrayList},
 * <strong>which will fail</strong> (though arguably the subtype was never part of the contract).
 *
 * <p>The callback ({@code someReporter} in the above example will be invoked if the returned {@link Map} is mutated at the top level; if
 * any values which are {@link Map}, {@link List}, or {@link Set} instances are mutated; and so on recursively inside nested {@link Map},
 * {@link List}, and {@link Set} structures. Mutating operations done via other types, e.g. via an array or a {@link java.util.Date}, will
 * <strong>not</strong> be reported.
 */
public class WriteReportingWrapper {

    private final Consumer<String> reporter;

    public static WriteReportingWrapper reportingWritesUsing(Consumer<String> reporter) {
        return new WriteReportingWrapper(reporter);
    }

    private WriteReportingWrapper(Consumer<String> reporter) {
        this.reporter = reporter;
    }

    public <K, V> Map<K, V> wrap(Map<K, V> source, String label) {
        @SuppressWarnings("unchecked") // safe because it returns a copy of the same type, with the caveats in the javadoc
        Map<K, V> copy = (Map<K, V>) wrapObject(source, label);
        return copy;
    }

    private Object wrapObject(Object value, String label) {
        switch (value) {
            case Map<?, ?> mapValue -> {
                Map<Object, Object> copy = Maps.newMapWithExpectedSize(mapValue.size());
                for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                    copy.put(entry.getKey(), wrapObject(entry.getValue(), label + "[" + entry.getKey() + "]"));
                }
                return new WriteReportingMap<>(copy, reporter, label);
            }
            case List<?> listValue -> {
                List<Object> copy = new ArrayList<>(listValue.size());
                int index = 0;
                for (Object itemValue : listValue) {
                    copy.add(wrapObject(itemValue, label + "[" + index++ + "]"));
                }
                return new WriteReportingList<>(copy, reporter, label);
            }
            case Set<?> setValue -> {
                Set<Object> copy = Sets.newHashSetWithExpectedSize(setValue.size());
                int index = 0;
                for (Object itemValue : setValue) {
                    copy.add(wrapObject(itemValue, label + "[" + index++ + "]"));
                }
                return new WriteReportingSet<>(copy, reporter, label);
            }
            case null, default -> {
                return value;
            }
        }
    }
}
