/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * <p>This package exposes the core compute engine functionality.</p>
 *
 * The {@link org.elasticsearch.compute.data.Page} class is the batched columnar representation of data
 * that's passed around in the compute engine. Pages are immutable and thread-safe.
 * The {@link org.elasticsearch.compute.operator.Operator} interface is the low-level building block that consumes,
 * transforms and produces data in the compute engine.
 * Each {@link org.elasticsearch.compute.operator.Driver} operates in single-threaded fashion on a simple chain of
 * operators, passing pages from one operator to the next.
 *
 * Parallelization and distribution is achieved via data exchanges. An exchange connects sink and source operators from different drivers
 * (see {@link org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator} and
 * {@link org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator}).
 * Exchanges can be thought of as simple FIFO queues of pages
 * (see {@link org.elasticsearch.compute.operator.exchange.ExchangeSource}).
 * Their classes are generally thread-safe due to concurrent access.
 * Exchanges can be remote as well as local (only local implemented so far).
 * They allow multi-plexing via an exchange, broadcasting one
 * sink to multiple sources (e.g. partitioning the incoming data to multiple targets based on the value of a given field), or connecting
 * multiple sinks to a single source (merging subcomputations). Even if no multiplexing is happening, exchanges allow pipeline processing
 * (i.e. you can have two pipelines of operators that are connected via an exchange, allowing two drivers to work in parallel on each side
 * of the exchange, even on the same node). Each driver does not require a new thread, however, so you could still schedule the two drivers
 * to run with the same thread when resources are scarce.
 */
package org.elasticsearch.compute;
