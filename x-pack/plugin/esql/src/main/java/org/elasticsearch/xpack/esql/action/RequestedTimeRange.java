/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;

import java.time.Instant;

/**
 * The {@code @timestamp} range a request filter asks for, together with the now its date math was resolved against.
 * <p>
 * The now is the query's own, the one that {@code NOW()} and the filter on the data nodes use, so that the age of a range can be
 * derived without a second, slightly different notion of now: a {@code now-1h} lower bound is exactly one hour old.
 *
 * @param bounds the bounds, as planning uses them
 * @param now    the now that relative bounds such as {@code now-1h} were resolved against
 */
public record RequestedTimeRange(TimestampBounds bounds, Instant now) {}
