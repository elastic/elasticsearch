/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

public class SerializableString {
    private final String name;
    private volatile SerializedString serializedString = null;

    public static SerializableString create(String name) {
        return new SerializableString(name);
    }

    private SerializableString(String name) {
        this.name = name;
    }

    public SerializedString serialize(XContentGenerator generator) {
        if (serializedString == null) {
            serializedString = generator.serializeString(name);
        }
        return serializedString;
        // TODO: consider the case where there are multiple XContentGenerator#serializeString implementation.
        // E.g. have a tag unique for each #serializeString implementation,
        // and have a map like serializations.computeIfAbsent(tag, generator -> generator.serializeString(name));
    }
}
