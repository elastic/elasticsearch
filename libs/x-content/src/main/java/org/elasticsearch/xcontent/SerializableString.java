/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import java.util.function.Function;

public class SerializableString {
    private final String name;
    private volatile Object serializedString = null;

    public static SerializableString create(String name) {
        return new SerializableString(name);
    }

    private SerializableString(String name) {
        this.name = name;
    }

    public <T> T computeIfAbsent(Class<T> clazz, Function<String, T> supplier) {
        if (serializedString != null && serializedString.getClass().equals(clazz)) {
            return clazz.cast(serializedString);
        }
        var serializedString = supplier.apply(name);
        this.serializedString = serializedString;
        return serializedString;
    }
}
