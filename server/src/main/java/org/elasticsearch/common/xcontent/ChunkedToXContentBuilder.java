/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
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
        builder.add(Objects.requireNonNull(content));
    }

    public ToXContent.Params params() {
        return params;
    }

    private void startObject() {
        addChunk((b, p) -> b.startObject());
    }

    private void startObject(String name) {
        addChunk((b, p) -> b.startObject(name));
    }

    private void endObject() {
        addChunk((b, p) -> b.endObject());
    }

    /**
     * Creates an object, with the specified {@code contents}
     */
    public ChunkedToXContentBuilder xContentObject(ToXContent contents) {
        startObject();
        append(contents);
        endObject();
        return this;
    }

    /**
     * Creates an object named {@code name}, with the specified {@code contents}
     */
    public ChunkedToXContentBuilder xContentObject(String name, ToXContent contents) {
        startObject(name);
        append(contents);
        endObject();
        return this;
    }

    /**
     * Creates an object, with the specified {@code contents}
     */
    public ChunkedToXContentBuilder xContentObject(ChunkedToXContent contents) {
        startObject();
        append(contents);
        endObject();
        return this;
    }

    /**
     * Creates an object named {@code name}, with the specified {@code contents}
     */
    public ChunkedToXContentBuilder xContentObject(String name, ChunkedToXContent contents) {
        startObject(name);
        append(contents);
        endObject();
        return this;
    }

    /**
     * Creates an object, with the specified {@code contents}
     */
    public ChunkedToXContentBuilder xContentObject(Iterator<? extends ToXContent> contents) {
        startObject();
        append(contents);
        endObject();
        return this;
    }

    /**
     * Creates an object named {@code name}, with the specified {@code contents}
     */
    public ChunkedToXContentBuilder xContentObject(String name, Iterator<? extends ToXContent> contents) {
        startObject(name);
        append(contents);
        endObject();
        return this;
    }

    /**
     * Creates an object with the contents of each field created from each entry in {@code map}
     */
    public ChunkedToXContentBuilder xContentObjectFields(Map<String, ? extends ToXContent> map) {
        startObject();
        map.forEach(this::field);
        endObject();
        return this;
    }

    /**
     * Creates an object named {@code name}, with the contents of each field created from each entry in {@code map}
     */
    public ChunkedToXContentBuilder xContentObjectFields(String name, Map<String, ? extends ToXContent> map) {
        startObject(name);
        map.forEach(this::field);
        endObject();
        return this;
    }

    /**
     * Creates an object with the contents of each field each another object created from each entry in {@code map}
     */
    public ChunkedToXContentBuilder xContentObjectFieldObjects(Map<String, ? extends ToXContent> map) {
        startObject();
        map.forEach(this::xContentObject);
        endObject();
        return this;
    }

    /**
     * Creates an object named {@code name}, with the contents of each field each another object created from each entry in {@code map}
     */
    public ChunkedToXContentBuilder xContentObjectFieldObjects(String name, Map<String, ? extends ToXContent> map) {
        startObject(name);
        map.forEach(this::xContentObject);
        endObject();
        return this;
    }

    /**
     * Creates an object, with the contents set by {@code contents}
     */
    public ChunkedToXContentBuilder object(Consumer<ChunkedToXContentBuilder> contents) {
        startObject();
        execute(contents);
        endObject();
        return this;
    }

    /**
     * Creates an object named {@code name}, with the contents set by {@code contents}
     */
    public ChunkedToXContentBuilder object(String name, Consumer<ChunkedToXContentBuilder> contents) {
        startObject(name);
        execute(contents);
        endObject();
        return this;
    }

    /**
     * Creates an object, with the contents set by calling {@code create} on each item returned by {@code items}
     */
    public <T> ChunkedToXContentBuilder object(Iterator<T> items, BiConsumer<ChunkedToXContentBuilder, ? super T> create) {
        startObject();
        forEach(items, create);
        endObject();
        return this;
    }

    /**
     * Creates an object, with the contents set by appending together
     * the return values of {@code create} called on each item returned by {@code items}
     */
    public <T> ChunkedToXContentBuilder object(Iterator<T> items, Function<? super T, ToXContent> create) {
        startObject();
        forEach(items, create);
        endObject();
        return this;
    }

    /**
     * Creates an object named {@code name}, with the contents set by calling {@code create} on each item returned by {@code items}
     */
    public <T> ChunkedToXContentBuilder object(String name, Iterator<T> items, BiConsumer<ChunkedToXContentBuilder, ? super T> create) {
        startObject(name);
        forEach(items, create);
        endObject();
        return this;
    }

    /**
     * Creates an object named {@code name}, with the contents set by appending together
     * the return values of {@code create} called on each item returned by {@code items}
     */
    public <T> ChunkedToXContentBuilder object(String name, Iterator<T> items, Function<? super T, ToXContent> create) {
        startObject(name);
        forEach(items, create);
        endObject();
        return this;
    }

    /**
     * Creates an object with the contents of each field set by {@code map}
     */
    public ChunkedToXContentBuilder object(Map<String, ?> map) {
        startObject();
        map.forEach(this::field);
        endObject();
        return this;
    }

    /**
     * Creates an object named {@code name}, with the contents of each field set by {@code map}
     */
    public ChunkedToXContentBuilder object(String name, Map<String, ?> map) {
        startObject(name);
        map.forEach(this::field);
        endObject();
        return this;
    }

    private void startArray() {
        addChunk((b, p) -> b.startArray());
    }

    private void startArray(String name) {
        addChunk((b, p) -> b.startArray(name));
    }

    private void endArray() {
        addChunk((b, p) -> b.endArray());
    }

    public ChunkedToXContentBuilder array(String name, String[] values) {
        addChunk((b, p) -> b.array(name, values));
        return this;
    }

    /**
     * Creates an array, with the contents set by calling {@code create} on each item returned by {@code items}
     */
    public <T> ChunkedToXContentBuilder array(Iterator<T> items, BiConsumer<ChunkedToXContentBuilder, ? super T> create) {
        startArray();
        forEach(items, create);
        endArray();
        return this;
    }

    /**
     * Creates an array with the contents set by appending together the contents of {@code items}
     */
    public ChunkedToXContentBuilder array(Iterator<? extends ToXContent> items) {
        startArray();
        items.forEachRemaining(this::append);
        endArray();
        return this;
    }

    /**
     * Creates an array, with the contents set by appending together
     * the return values of {@code create} called on each item returned by {@code items}
     */
    public <T> ChunkedToXContentBuilder array(Iterator<T> items, Function<? super T, ToXContent> create) {
        startArray();
        forEach(items, create);
        endArray();
        return this;
    }

    /**
     * Creates an array named {@code name}, with the contents set by calling {@code create} on each item returned by {@code items}
     */
    public <T> ChunkedToXContentBuilder array(String name, Iterator<T> items, BiConsumer<ChunkedToXContentBuilder, ? super T> create) {
        startArray(name);
        forEach(items, create);
        endArray();
        return this;
    }

    /**
     * Creates an array named {@code name}, with the contents set by appending together the contents of {@code items}
     */
    public ChunkedToXContentBuilder array(String name, Iterator<? extends ToXContent> items) {
        startArray(name);
        items.forEachRemaining(this::append);
        endArray();
        return this;
    }

    /**
     * Creates an array named {@code name}, with the contents set by appending together
     * the return values of {@code create} called on each item returned by {@code items}
     */
    public <T> ChunkedToXContentBuilder array(String name, Iterator<T> items, Function<? super T, ToXContent> create) {
        startArray(name);
        forEach(items, create);
        endArray();
        return this;
    }

    public ChunkedToXContentBuilder field(String name) {
        addChunk((b, p) -> b.field(name));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, boolean value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, Boolean value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, int value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, Integer value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, long value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, Long value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, float value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, Float value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, double value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, Double value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, String value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, Enum<?> value) {
        addChunk((b, p) -> b.field(name, value));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, ToXContent value) {
        addChunk((b, p) -> b.field(name, value, p));
        return this;
    }

    public ChunkedToXContentBuilder field(String name, ChunkedToXContent value) {
        addChunk((b, p) -> b.field(name));
        append(value);
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

    /**
     * Adds chunks from an iterator. Each item is passed to {@code create} to add chunks to this builder.
     */
    public <T> ChunkedToXContentBuilder forEach(Iterator<T> items, BiConsumer<ChunkedToXContentBuilder, ? super T> create) {
        items.forEachRemaining(t -> create.accept(this, t));
        return this;
    }

    /**
     * Adds chunks from an iterator. Each item is passed to {@code create}, and the resulting {@code ToXContent} objects
     * are added to this builder in order.
     */
    public <T> ChunkedToXContentBuilder forEach(Iterator<T> items, Function<? super T, ToXContent> create) {
        items.forEachRemaining(t -> append(create.apply(t)));
        return this;
    }

    public ChunkedToXContentBuilder append(ToXContent xContent) {
        if (xContent != ToXContent.EMPTY) {
            addChunk(xContent);
        }
        return this;
    }

    public ChunkedToXContentBuilder append(ChunkedToXContent chunk) {
        if (chunk != ChunkedToXContent.EMPTY) {
            append(chunk.toXContentChunked(params));
        }
        return this;
    }

    public ChunkedToXContentBuilder append(Iterator<? extends ToXContent> xContents) {
        xContents.forEachRemaining(this::append);
        return this;
    }

    public ChunkedToXContentBuilder appendIfPresent(@Nullable ToXContent xContent) {
        if (xContent != null) {
            append(xContent);
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
