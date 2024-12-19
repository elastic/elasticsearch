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
import java.util.function.Function;

import static java.util.Collections.emptyIterator;

public class NewChunkedXContentBuilder {

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

    public static Iterator<? extends ToXContent> empty() {
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

    public static Iterator<? extends ToXContent> ifThen(boolean condition, ToXContent content) {
        return condition ? chunk(content) : emptyIterator();
    }

    public static Iterator<? extends ToXContent> ifThen(boolean condition, Iterator<? extends ToXContent> content) {
        return condition ? content : emptyIterator();
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

    public static <T> Iterator<? extends ToXContent> object(String name, Iterator<T> items, Function<? super T, ToXContent> create) {
        return of(startObject(name), Iterators.map(items, create), endObject());
    }

    public static Iterator<? extends ToXContent> array(String name, Iterator<? extends ToXContent> items) {
        return of(startArray(name), items, endArray());
    }

    public static <T> Iterator<? extends ToXContent> array(Iterator<T> items, Function<? super T, ToXContent> create) {
        return of(startArray(), Iterators.map(items, create), endArray());
    }

    public static <T> Iterator<? extends ToXContent> array(String name, Iterator<T> items, Function<? super T, ToXContent> create) {
        return of(startArray(name), Iterators.map(items, create), endArray());
    }

    public static Iterator<? extends ToXContent> xContentObject(String name, Iterator<? extends ToXContent> contents) {
        return of(startObject(name), contents, endObject());
    }

    public static <T> Iterator<? extends ToXContent> forEach(Iterator<T> items, Function<T, Iterator<? extends ToXContent>> map) {
        return Iterators.flatMap(items, map);
    }
}
