/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * A page iterator that also hands out the element pages of the {@link ChildRelationRead child relations} its read
 * asked for. The elements of a parent page are decoded with it, in the same pass, so a consumer reads them without a
 * second scan of the file and without the collection ever becoming a column of the parent page.
 * <p>
 * A wrapper between the reader and the consumer implements this too and forwards to what it wraps, renumbering the
 * parents when it drops rows. A wrapper over an iterator that does not implement it fails through
 * {@link #forward}, rather than answering with no elements, which would read as an empty collection.
 */
public interface ChildRelationPages {

    /**
     * The elements of collection {@code path} for the parent page most recently returned by {@code next()} or
     * {@code tryAdvance()}. Asking twice for one page returns the same windows. They stay valid until the consumer
     * closes them or the next parent page is taken, whichever comes first; the iterator closes them then.
     *
     * @throws IllegalArgumentException when the read did not ask for {@code path}
     * @throws IllegalStateException when no parent page has been returned yet
     */
    ChildWindows children(String path);

    /**
     * {@code delegate}'s windows for {@code path}, for a wrapper that implements this interface over another iterator.
     *
     * @throws IllegalStateException when {@code delegate} reads no child relations
     */
    static ChildWindows forward(Object delegate, String path) {
        if (delegate instanceof ChildRelationPages pages) {
            return pages.children(path);
        }
        throw new IllegalStateException(
            "child relation [" + path + "] requested but the underlying iterator [" + delegate.getClass().getName() + "] reads none"
        );
    }
}
