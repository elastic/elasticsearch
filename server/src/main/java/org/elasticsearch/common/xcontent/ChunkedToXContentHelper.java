/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    public static Iterator<ToXContent> map(String name, Map<String, ?> map) {
        return map(name, map, entry -> (ToXContent) (builder, params) -> builder.field(entry.getKey(), entry.getValue()));
    }

    public static Iterator<ToXContent> xContentFragmentValuesMap(String name, Map<String, ? extends ToXContent> map) {
        return map(
            name,
            map,
            entry -> (ToXContent) (builder, params) -> entry.getValue().toXContent(builder.startObject(entry.getKey()), params).endObject()
        );
    }

    public static Iterator<ToXContent> xContentValuesMap(String name, Map<String, ? extends ToXContent> map) {
        return map(
            name,
            map,
            entry -> (ToXContent) (builder, params) -> entry.getValue().toXContent(builder.field(entry.getKey()), params)
        );
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
        return Iterators.concat(ChunkedToXContentHelper.startArray(name), contents, ChunkedToXContentHelper.endArray());
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
        return Iterators.concat(
            ChunkedToXContentHelper.startArray(name),
            Iterators.flatMap(contents, c -> c.toXContentChunked(params)),
            ChunkedToXContentHelper.endArray()
        );
    }

    public static <T extends ToXContent> Iterator<ToXContent> wrapWithObject(String name, Iterator<T> iterator) {
        return Iterators.concat(startObject(name), iterator, endObject());
    }

    public static <T> Iterator<ToXContent> map(String name, Map<String, T> map, Function<Map.Entry<String, T>, ToXContent> toXContent) {
        return wrapWithObject(name, Iterators.map(map.entrySet().iterator(), toXContent));
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
