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

public class NewChunkedXContentBuilder {

    private static ToXContent startObject() {
        return (b, p) -> b.startObject();
    }

    private static ToXContent startObject(String name) {
        return (b, p) -> b.startObject(name);
    }

    private static ToXContent endObject() {
        return (b, p) -> b.endObject();
    }

    private static ToXContent startArray() {
        return (b, p) -> b.startArray();
    }

    private static ToXContent startArray(String name) {
        return (b, p) -> b.startArray(name);
    }

    private static ToXContent endArray() {
        return (b, p) -> b.endArray();
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
        return of(of(startObject()), of(items), of(endObject()));
    }

    public static Iterator<? extends ToXContent> object(Iterator<? extends ToXContent> items) {
        return of(of(startObject()), items, of(endObject()));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static Iterator<? extends ToXContent> object(Iterator<? extends ToXContent>... items) {
        return of(of(startObject()), of(items), of(endObject()));
    }

    public static Iterator<? extends ToXContent> array(String name, Iterator<? extends ToXContent> items) {
        return of(of(startArray(name)), items, of(endArray()));
    }
}
