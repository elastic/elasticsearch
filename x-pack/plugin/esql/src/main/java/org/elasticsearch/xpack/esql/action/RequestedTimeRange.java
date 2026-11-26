/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.Nullable;

import java.time.Instant;

/**
 * The {@code @timestamp} range a query asks for, through its request filter and the filters in the query itself such as
 * {@code WHERE @timestamp >= NOW() - 1 hour}, together with the now that relative bounds were resolved against.
 * <p>
 * The now is the query's own, the one that {@code NOW()} and the filter on the data nodes use, so that the age of a range can be
 * derived without a second, slightly different notion of now: a {@code now-1h} lower bound is exactly one hour old.
 * <p>
 * It is meant for observability, not for planning: both ends are inclusive at millisecond resolution, and a query that reads
 * several relations is reduced to the one range that covers them all. A view that redefines {@code @timestamp} after filtering
 * on it is not accounted for.
 *
 * @param from the lower bound, or {@code null} if the query sets none
 * @param to   the upper bound, or {@code null} if the query sets none
 * @param now  the now that relative bounds such as {@code now-1h} were resolved against
 */
public record RequestedTimeRange(@Nullable Instant from, @Nullable Instant to, Instant now) {}
