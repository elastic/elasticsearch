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
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A fluent builder to create {@code Iterator&lt;ToXContent&gt;} objects
 */
public class ChunkedToXContentBuilder implements Iterator<ToXContent> {

    private final ToXContent.Params params;
    private final Stream.Builder<ToXContent> builder = Stream.builder();
    private Iterator<ToXContent> iterator;

    public ChunkedToXContentBuilder(ToXContent.Params params) {
        this.params = params;
    }

    private void addChunk(ToXContent content) {
        assert iterator == null : "Builder has been read, cannot add any more chunks";
        builder.add(content);
    }

    public ChunkedToXContentBuilder startObject() {
        addChunk((b, p) -> b.startObject());
        return this;
    }

    public ChunkedToXContentBuilder startObject(String name) {
        addChunk((b, p) -> b.startObject(name));
        return this;
    }

    public ChunkedToXContentBuilder endObject() {
        addChunk((b, p) -> b.endObject());
        return this;
    }

    public ChunkedToXContentBuilder object(String name, Consumer<ChunkedToXContentBuilder> contents) {
        return startObject(name).execute(contents).endObject();
    }

    public ChunkedToXContentBuilder startArray() {
        addChunk((b, p) -> b.startArray());
        return this;
    }

    public ChunkedToXContentBuilder startArray(String name) {
        addChunk((b, p) -> b.startArray(name));
        return this;
    }

    public ChunkedToXContentBuilder endArray() {
        addChunk((b, p) -> b.endArray());
        return this;
    }

    public ChunkedToXContentBuilder array(String name, String... values) {
        addChunk((b, p) -> b.array(name, values));
        return this;
    }

    public ChunkedToXContentBuilder field(String name) {
        addChunk((b, p) -> b.field(name));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, long value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, String value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, Object value) {
        addChunk((b, p) -> b.field(name, value));
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

    public <T> ChunkedToXContentBuilder forEach(Iterator<T> items, BiConsumer<? super T, ChunkedToXContentBuilder> create) {
        items.forEachRemaining(t -> create.accept(t, this));
        return this;
    }

    public <T> ChunkedToXContentBuilder forEach(
        Iterator<T> items,
        Function<? super T, CheckedBiConsumer<XContentBuilder, ToXContent.Params, IOException>> create
    ) {
        items.forEachRemaining(t -> append(create.apply(t)));
        return this;
    }

    public <T> ChunkedToXContentBuilder appendEntries(Map<String, ?> map) {
        return forEach(map.entrySet().iterator(), (e, b) -> b.field(e.getKey(), e.getValue()));
    }

    public <T> ChunkedToXContentBuilder appendXContentObjects(Map<String, ? extends ToXContent> map) {
        return forEach(map.entrySet().iterator(), (e, b) -> b.startObject(e.getKey()).appendXContent(e.getValue()).endObject());
    }

    public <T> ChunkedToXContentBuilder appendXContentFields(Map<String, ? extends ToXContent> map) {
        return forEach(map.entrySet().iterator(), (e, b) -> b.field(e.getKey()).appendXContent(e.getValue()));
    }

    private ChunkedToXContentBuilder appendXContent(ToXContent xContent) {
        if (xContent != ToXContent.EMPTY) {
            addChunk(xContent);
        }
        return this;
    }

    /**
     * Adds an {@code ToXContent}-like object to the builder.
     * <p>
     * This uses a straight {@code BiConsumer} rather than {@code ToXContent} here
     * so the lambda is not forced to return the builder back to us.
     */
    public ChunkedToXContentBuilder append(CheckedBiConsumer<XContentBuilder, ToXContent.Params, IOException> xContent) {
        addChunk((b, p) -> {
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
