/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Iterator;

public enum ChunkedToXContentHelper {
    ;

    public static Iterator<ToXContent> startObject() {
        return Iterators.single(((builder, params) -> builder.startObject()));
    }

    public static Iterator<ToXContent> startObject(String name) {
        return Iterators.single(((builder, params) -> builder.startObject(name)));
    }

    public static Iterator<ToXContent> endObject() {
        return Iterators.single(((builder, params) -> builder.endObject()));
    }

    public static Iterator<ToXContent> startArray() {
        return Iterators.single(((builder, params) -> builder.startArray()));
    }

    public static Iterator<ToXContent> startArray(String name) {
        return Iterators.single(((builder, params) -> builder.startArray(name)));
    }

    public static Iterator<ToXContent> endArray() {
        return Iterators.single(((builder, params) -> builder.endArray()));
    }

    public static Iterator<ToXContent> field(String name, boolean value) {
        return Iterators.single(((builder, params) -> builder.field(name, value)));
    }

    public static Iterator<ToXContent> field(String name, long value) {
        return Iterators.single(((builder, params) -> builder.field(name, value)));
    }

    public static Iterator<ToXContent> field(String name, String value) {
        return Iterators.single(((builder, params) -> builder.field(name, value)));
    }

    /**
     * Creates an Iterator of a single ToXContent object that serializes the given object as a single chunk. Just wraps {@link
     * Iterators#single}, but still useful because it avoids any type ambiguity.
     *
     * @param item Item to wrap
     * @return Singleton iterator for the given item.
     */
    public static Iterator<ToXContent> singleChunk(ToXContent item) {
        return Iterators.single(item);
    }
}
