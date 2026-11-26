/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Turns a drained {@link UsageSnapshot} into the {@link UsageRecord}s to send. Deterministic: the same snapshot and identity give the same
 * records with the same ids, which is what makes a retry a duplicate rather than a double count. It builds one record per outcome
 * with non-zero usage and nothing at all when the identity is not {@link SourceIdentity#isComplete() complete}, since the Usage API
 * would reject the batch.
 */
public interface UsageRecordBuilder {

    /**
     * @param period the span of time that the snapshot covers, ending at its drain time: the caller passes the real window (the time
     *               since the previous drain, or since the start for the first one), not the configured interval. The records are
     *               stamped with the start of that window and carry its length, so a partial drain, such as the final one on shutdown,
     *               is not attributed to a day or month before the one its usage was collected in.
     */
    List<UsageRecord> build(UsageSnapshot snapshot, SourceIdentity identity, Duration period, Instant creationTime);
}
