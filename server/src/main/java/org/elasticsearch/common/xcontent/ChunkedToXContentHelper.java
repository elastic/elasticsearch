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
import java.util.Map;
import java.util.function.Function;

public enum ChunkedToXContentHelper {
    ;

    public static Iterator<ToXContent> startObject() {
        return Iterators.single((b, p) -> b.startObject());
    }

    public static Iterator<ToXContent> startObject(String name) {
        return Iterators.single((b, p) -> b.startObject(name));
    }

    public static Iterator<ToXContent> endObject() {
        return Iterators.single((b, p) -> b.endObject());
    }

    public static Iterator<ToXContent> startArray() {
        return Iterators.single((b, p) -> b.startArray());
    }

    public static Iterator<ToXContent> startArray(String name) {
        return Iterators.single((b, p) -> b.startArray(name));
    }

    public static Iterator<ToXContent> endArray() {
        return Iterators.single((b, p) -> b.endArray());
    }

    /**
     * Defines an object named {@code name}, with the contents of each field set by {@code map}
     */
    public static Iterator<ToXContent> object(String name, Map<String, ?> map) {
        return object(name, map, e -> (b, p) -> b.field(e.getKey(), e.getValue()));
    }

    /**
     * Defines an object named {@code name}, with the contents set by calling {@code toXContent} on each entry in {@code map}
     */
    public static <T> Iterator<ToXContent> object(String name, Map<String, T> map, Function<Map.Entry<String, T>, ToXContent> toXContent) {
        return object(name, Iterators.map(map.entrySet().iterator(), toXContent));
    }

    /**
     * Defines an object named {@code name}, with the contents of each field created from each entry in {@code map}
     */
    public static Iterator<ToXContent> xContentObjectFields(String name, Map<String, ? extends ToXContent> map) {
        return object(name, map, e -> (b, p) -> e.getValue().toXContent(b.field(e.getKey()), p));
    }

    /**
     * Defines an object named {@code name}, with the contents of each field each another object created from each entry in {@code map}
     */
    public static Iterator<ToXContent> xContentObjectFieldObjects(String name, Map<String, ? extends ToXContent> map) {
        return object(name, map, e -> (b, p) -> e.getValue().toXContent(b.startObject(e.getKey()), p).endObject());
    }

    /**
     * Creates an Iterator to serialize a named field where the value is represented by a {@link ChunkedToXContentObject}.
     * Chunked equivalent for {@code XContentBuilder field(String name, ToXContent value)}
     * @param name name of the field
     * @param value value for this field
     * @param params params to propagate for XContent serialization
     * @return Iterator composing field name and value serialization
     */
    public static Iterator<ToXContent> field(String name, ChunkedToXContentObject value, ToXContent.Params params) {
        return Iterators.concat(Iterators.single((builder, innerParam) -> builder.field(name)), value.toXContentChunked(params));
    }

    public static Iterator<ToXContent> array(String name, Iterator<? extends ToXContent> contents) {
        return Iterators.concat(startArray(name), contents, endArray());
    }

    public static <T> Iterator<ToXContent> array(Iterator<T> items, Function<T, ToXContent> toXContent) {
        return Iterators.concat(startArray(), Iterators.map(items, toXContent), endArray());
    }

    /**
     * Creates an Iterator to serialize a named field where the value is represented by an iterator of {@link ChunkedToXContentObject}.
     * Chunked equivalent for {@code XContentBuilder array(String name, ToXContent value)}
     * @param name name of the field
     * @param contents values for this field
     * @param params params to propagate for XContent serialization
     * @return Iterator composing field name and value serialization
     */
    public static Iterator<ToXContent> array(String name, Iterator<? extends ChunkedToXContentObject> contents, ToXContent.Params params) {
        return array(name, Iterators.flatMap(contents, c -> c.toXContentChunked(params)));
    }

    /**
     * Defines an object named {@code name}, with the contents set by {@code iterator}
     */
    public static Iterator<ToXContent> object(String name, Iterator<? extends ToXContent> iterator) {
        return Iterators.concat(startObject(name), iterator, endObject());
    }

    /**
     * Creates an Iterator of a single ToXContent object that serializes the given object as a single chunk. Just wraps {@link
     * Iterators#single}, but still useful because it avoids any type ambiguity.
     *
     * @param item Item to wrap
     * @return Singleton iterator for the given item.
     */
    public static Iterator<ToXContent> chunk(ToXContent item) {
        return Iterators.single(item);
    }

}
