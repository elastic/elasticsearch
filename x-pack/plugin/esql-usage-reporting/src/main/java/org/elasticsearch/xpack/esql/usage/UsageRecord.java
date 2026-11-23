/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import java.time.Instant;
import java.util.Map;

/**
 * One usage record as the Usage API receives it, and the unit that is summed downstream.
 * <p>
 * The {@code id} is stable: a record that is sent again after a failure carries the same id, so the Usage API sees a duplicate
 * and never counts it twice. This is the guard against over-counting. {@code usageTimestamp} is the start of the period that the
 * quantity covers, because billing attributes a record to the range that contains it. {@code quantityMillis} is the counted CPU time of
 * {@code periodSeconds} seconds of activity of one node, in milliseconds. {@code usageType} must match, character for
 * character, the key of the transform that interprets it (see {@link UsageTypes}). {@code exemption} is null for counted usage; usage
 * of queries that did not succeed is exported in separate records that carry an {@link Exemption} and is never mixed into a counted
 * record, because a record has a single exemption flag.
 */
public record UsageRecord(
    String id,
    Instant usageTimestamp,
    Instant creationTimestamp,
    String usageType,
    long quantityMillis,
    long periodSeconds,
    Map<String, String> metadata,
    Exemption exemption,
    SourceIdentity source
) {

    /**
     * Marks a record as not counted and says why: {@code "failed"} or {@code "cancelled"}.
     */
    public record Exemption(String reason) {}
}
