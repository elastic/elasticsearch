/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import java.util.function.Function;

/**
 * Abstraction over lazily serialized String tokens/Symbols.
 * Holds a concrete class from a serialization/content generation framework (e.g. Jackson) that can lazily serialize the contained
 * String {@code value} and then reuse that serialization later on. Instances should only be created when they are used more than once;
 * prime candidates are field names.
 */
public class SerializableString {

    /**
     * A wrapper to make the publication of the reference safe, so that the lazy initialization or
     * {@link SerializableString} works with concurrent access without being volatile.
     * See <a href="https://shipilev.net/blog/2014/safe-public-construction/">Safe Publication and Safe Initialization in Java</a>
     */
    private static class FinalWrapper {
        public final Object serializedStringValue;

        FinalWrapper(Object serializedStringValue) {
            this.serializedStringValue = serializedStringValue;
        }
    }

    private FinalWrapper wrappedSerializedValue;
    private final String value;

    public static SerializableString of(String name) {
        return new SerializableString(name);
    }

    private SerializableString(String value) {
        this.value = value;
    }

    /**
     * Get the cached serialized String or, if not present, creates a serialization and stores it for later reuse.
     */
    public <T> T computeIfAbsent(Class<T> clazz, Function<String, T> factory) {
        // This un-synchronized read is racy, but it's the only one, and we address reading null for variable `v` later;
        // It's important to keep this as the unique un-synchronized read, don't inline it
        var v = this.wrappedSerializedValue;
        if (v == null) {
            synchronized (this) {
                // Re-reading and assigning v inside the synchronized block so it's not racy
                v = this.wrappedSerializedValue;
                if (v == null) {
                    v = new FinalWrapper(factory.apply(value));
                    this.wrappedSerializedValue = v;
                }
            }
        }
        return clazz.cast(v.serializedStringValue);
    }

    public String stringValue() {
        return value;
    }
}
