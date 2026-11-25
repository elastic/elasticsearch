/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Reports ES|QL Data Federation usage to the Elastic Usage API on hosted deployments.
 * <p>
 * Two threads of control meet only in the {@link org.elasticsearch.xpack.esql.usage.UsageAccumulator}:
 * <pre>
 * query thread (coordinating node)               reporter thread (one per node), every period
 *   query completes                                tick(now), aligned to wall-clock multiples of the period
 *     UsageListener.onQueryCompleted                1. snapshot = accumulator.drain(now)         (resets the sums)
 *       UsageAccumulator.record(outcome, m)         2. records  = recordBuilder.build(snapshot, identity, period, now)
 *         (atomic additions only)                   3. pendingStore.add(records)
 *                                                   4. loop: batch = pendingStore.next(batchSize)
 *                                                            result = publisher.publish(batch)
 *                                                            accepted: acknowledge; INVALID: drop; RETRYABLE or AUTH: keep, stop
 * </pre>
 * Nothing on the query thread does I/O, takes a lock or can fail the query.
 * <p>
 * <b>Over-counting is the worst error</b>; usage lost to a crash or to a bound is under-counted, which is accepted and logged. That is
 * why record ids are stable (a retry is a duplicate), a bad record is isolated and dropped alone, and the sub-millisecond
 * remainder is dropped instead of rounded up.
 * <p>
 * Each piece is an interface with a no-op or simple implementation here, and {@link org.elasticsearch.xpack.esql.usage.UsageComponents}
 * has one factory method per piece, where the real implementation is plugged in: the accumulator, the record builder and its JSON
 * format, the pending store, the publisher and the reporter.
 */
package org.elasticsearch.xpack.esql.usage;
