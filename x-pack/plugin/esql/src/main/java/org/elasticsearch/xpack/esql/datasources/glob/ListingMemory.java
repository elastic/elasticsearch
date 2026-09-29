/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

/**
 * Reserves heap for the entries a listing is about to retain.
 * <p>
 * A listing's size is not known until it has been listed, so the only way to reserve before allocating is to
 * reserve as it grows. This is called during the walk,every batch of entries, <em>before</em> they are counted as
 * retained - so a dataset larger than the node can hold trips the breaker partway through its own listing rather
 * than after it has already been built.
 * <p>
 * Implementations throw {@code CircuitBreakingException} to refuse. Reserving is one-way: what is reserved is
 * released by whatever owns the reservation, never by the walk, which does not know when the listing it produced
 * stops being referenced.
 */
@FunctionalInterface
public interface ListingMemory {

    /** Reserves nothing and refuses nothing, for callers with no reservation to draw on. */
    ListingMemory NONE = bytes -> {};

    /**
     * Reserves {@code bytes} against whatever budget this draws on.
     *
     * @throws org.elasticsearch.common.breaker.CircuitBreakingException when the budget cannot afford them
     */
    void reserve(long bytes);
}
