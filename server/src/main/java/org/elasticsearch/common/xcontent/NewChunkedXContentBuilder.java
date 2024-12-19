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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.collect.Iterators.map;

public class NewChunkedXContentBuilder {
    // TODO: merge with ChunkedXContentHelper
    private static Iterator<? extends ToXContent> startObject() {
        return chunk((b, p) -> b.startObject());
    }

    private static Iterator<? extends ToXContent> startObject(String name) {
        return chunk((b, p) -> b.startObject(name));
    }

    private static Iterator<? extends ToXContent> endObject() {
        return chunk((b, p) -> b.endObject());
    }

    private static Iterator<? extends ToXContent> startArray() {
        return chunk((b, p) -> b.startArray());
    }

    private static Iterator<? extends ToXContent> startArray(String name) {
        return chunk((b, p) -> b.startArray(name));
    }

    private static Iterator<? extends ToXContent> endArray() {
        return chunk((b, p) -> b.endArray());
    }

    public static Iterator<ToXContent> empty() {
        return Collections.emptyIterator();
    }

    public static Iterator<? extends ToXContent> of(Iterator<? extends ToXContent> chunk) {
        return chunk;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static Iterator<? extends ToXContent> of(Iterator<? extends ToXContent>... chunks) {
        return Iterators.concat(chunks);
    }

    public static Iterator<? extends ToXContent> chunk(ToXContent xContent) {
        return of(xContent);
    }

    public static Iterator<? extends ToXContent> of(ToXContent xContent) {
        return Iterators.single(xContent);
    }

    public static Iterator<? extends ToXContent> of(ToXContent... chunks) {
        return Iterators.forArray(chunks);
    }

    public static Iterator<? extends ToXContent> object(ToXContent xContent) {
        return of((b, p) -> xContent.toXContent(b.startObject(), p).endObject());
    }

    public static Iterator<? extends ToXContent> object(ToXContent... items) {
        return of(startObject(), of(items), endObject());
    }

    public static Iterator<? extends ToXContent> object(Iterator<? extends ToXContent> items) {
        return of(startObject(), items, endObject());
    }

    public static Iterator<? extends ToXContent> object(String name, Iterator<? extends ToXContent> items) {
        return of(startObject(name), items, endObject());
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static Iterator<? extends ToXContent> object(Iterator<? extends ToXContent>... items) {
        return of(startObject(), of(items), endObject());
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static Iterator<? extends ToXContent> object(String name, Iterator<? extends ToXContent>... items) {
        return of(startObject(name), of(items), endObject());
    }

    public static Iterator<? extends ToXContent> array(Iterator<? extends ToXContent> items) {
        return of(startArray(), items, endArray());
    }

    public static Iterator<? extends ToXContent> array(String name, Iterator<? extends ToXContent> items) {
        return of(startArray(name), items, endArray());
    }

    /**
     * Creates an object named {@code name}, with the contents of each field created from each entry in {@code map}
     */
    public static Iterator<? extends ToXContent> xContentObjectFields(String name, Map<String, ? extends ToXContent> map) {
        return of(startObject(name), map(map.entrySet().iterator(), e -> (b, p) -> b.field(e.getKey(), e.getValue(), p)), endObject());
    }
}
