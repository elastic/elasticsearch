/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import java.util.List;
import java.util.Set;

/**
 * A {@link UsagePublisher} that sends nothing, so records stay pending. For a non-empty batch it reports a retryable failure of the
 * whole batch, so that a reporter that is wired up before the real publisher exists stops its cycle instead of asking for the
 * same batch again.
 */
public final class NoopUsagePublisher implements UsagePublisher {

    public static final NoopUsagePublisher INSTANCE = new NoopUsagePublisher();

    private NoopUsagePublisher() {}

    @Override
    public PublishResult publish(List<UsageRecord> records) {
        if (records.isEmpty()) {
            return PublishResult.empty();
        }
        return new PublishResult(
            Set.of(),
            List.of(new PublishResult.Failure(Set.of(), PublishResult.FailureClass.RETRYABLE, "no usage publisher is configured"))
        );
    }

    @Override
    public void close() {}
}
