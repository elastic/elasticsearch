/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A fluent builder to create {@code Iterator&lt;ToXContent&gt;} objects
 */
public class ChunkedToXContentBuilder implements Iterator<ToXContent> {

    public static ChunkedToXContentBuilder builder(ToXContent.Params params) {
        return new ChunkedToXContentBuilder(params);
    }

    private final ToXContent.Params params;
    private final Stream.Builder<ToXContent> builder = Stream.builder();
    private Iterator<ToXContent> iterator;

    public ChunkedToXContentBuilder(ToXContent.Params params) {
        this.params = params;
    }

    public ChunkedToXContentBuilder startObject() {
        builder.add((b, p) -> b.startObject());
        return this;
    }

    public ChunkedToXContentBuilder startObject(String name) {
        builder.add((b, p) -> b.startObject(name));
        return this;
    }

    public ChunkedToXContentBuilder endObject() {
        builder.add((b, p) -> b.endObject());
        return this;
    }

    public ChunkedToXContentBuilder startArray() {
        builder.add((b, p) -> b.startArray());
        return this;
    }

    public ChunkedToXContentBuilder startArray(String name) {
        builder.add((b, p) -> b.startArray(name));
        return this;
    }

    public ChunkedToXContentBuilder endArray() {
        builder.add((b, p) -> b.endArray());
        return this;
    }

    public ChunkedToXContentBuilder array(String name, String... values) {
        builder.add((b, p) -> b.array(name, values));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, long value) {
        builder.add((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, String value) {
        builder.add((b, p) -> b.field(name, value));
        return this;
    }

    public <T> ChunkedToXContentBuilder forEach(Iterator<T> items, BiConsumer<T, ChunkedToXContentBuilder> create) {
        items.forEachRemaining(t -> create.accept(t, this));
        return this;
    }

    private ChunkedToXContentBuilder appendXContent(ToXContent xContent) {
        if (xContent != ToXContent.EMPTY) {
            builder.add(xContent);
        }
        return this;
    }

    /**
     * Passes this object to {@code consumer}.
     * <p>
     * This may seem superfluous, but it allows callers to 'break out' into more declarative code
     * inside the lambda, whilst maintaining the top-level fluent method calls.
     */
    public ChunkedToXContentBuilder execute(Consumer<ChunkedToXContentBuilder> consumer) {
        consumer.accept(this);
        return this;
    }

    /**
     * Adds an {@code ToXContent}-like object to the builder.
     * <p>
     * This uses a straight {@code BiConsumer} rather than {@code ToXContent} here
     * so the lambda is not forced to return the builder back to us.
     */
    public ChunkedToXContentBuilder append(CheckedBiConsumer<XContentBuilder, ToXContent.Params, IOException> xContent) {
        builder.add((b, p) -> {
            xContent.accept(b, p);
            return b;
        });
        return this;
    }

    public ChunkedToXContentBuilder append(ChunkedToXContent chunk) {
        if (chunk != ChunkedToXContent.EMPTY) {
            append(chunk.toXContentChunked(params));
        }
        return this;
    }

    public ChunkedToXContentBuilder append(Iterator<? extends ToXContent> xContents) {
        xContents.forEachRemaining(this::appendXContent);
        return this;
    }

    public ChunkedToXContentBuilder appendIfPresent(@Nullable ToXContent xContent) {
        if (xContent != null) {
            appendXContent(xContent);
        }
        return this;
    }

    public ChunkedToXContentBuilder appendIfPresent(@Nullable ChunkedToXContent chunk) {
        if (chunk != null) {
            append(chunk);
        }
        return this;
    }

    private Iterator<ToXContent> checkCreateIterator() {
        if (iterator == null) {
            iterator = builder.build().iterator();
        }
        return iterator;
    }

    @Override
    public boolean hasNext() {
        return checkCreateIterator().hasNext();
    }

    @Override
    public ToXContent next() {
        return checkCreateIterator().next();
    }

    @Override
    public void forEachRemaining(Consumer<? super ToXContent> action) {
        checkCreateIterator().forEachRemaining(action);
    }
}
