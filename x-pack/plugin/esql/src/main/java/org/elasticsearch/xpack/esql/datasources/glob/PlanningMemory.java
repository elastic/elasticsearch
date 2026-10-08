/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

/**
 * Reserves coordinator heap a query is about to hold while planning, against whatever budget the caller named.
 * <p>
 * Two kinds of allocation go through it, and what they share is the budget and the direction, not the timing.
 * A listing reserves as it grows, every batch of entries, <em>before</em> they are counted as retained - a
 * listing's size is not known until it has been listed, so reserving as it grows is the only way to reserve
 * before allocating, and a dataset larger than the node can hold then trips partway through its own listing
 * rather than after the list exists. Everything sized from a completed listing reserves once, from that count,
 * before the structures are built.
 * <p>
 * Implementations throw {@code CircuitBreakingException} to refuse. Reserving is one-way: what is reserved is
 * released by whatever owns the reservation, never by the caller, which does not know when what it allocated
 * stops being referenced.
 */
@FunctionalInterface
public interface PlanningMemory {

    /** Reserves nothing and refuses nothing, for callers with no reservation to draw on. */
    PlanningMemory NONE = bytes -> {};

    /**
     * Reserves {@code bytes} against whatever budget this draws on.
     *
     * @throws org.elasticsearch.common.breaker.CircuitBreakingException when the budget cannot afford them
     */
    void reserve(long bytes);
}
