/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ssl.SslProfile;

import java.time.Clock;

/**
 * One factory method per piece of the collector, so that the changes that implement the pieces in parallel each replace the body
 * of one method and do not conflict. Until then every method returns the no-op or simple implementation: nothing is reported.
 */
public final class UsageComponents {

    private UsageComponents() {}

    public static UsageAccumulator newAccumulator(UsageReportingSettings settings) {
        return new AtomicUsageAccumulator();
    }

    public static UsageRecordBuilder newRecordBuilder(UsageReportingSettings settings) {
        return NoopUsageRecordBuilder.INSTANCE;
    }

    public static PendingStore newPendingStore(UsageReportingSettings settings) {
        return new BoundedPendingStore(settings.pendingMax());
    }

    public static UsagePublisher newPublisher(UsageReportingSettings settings, SslProfile sslProfile) {
        return NoopUsagePublisher.INSTANCE;
    }

    public static UsageReporter newReporter(
        UsageReportingSettings settings,
        UsageAccumulator accumulator,
        UsageRecordBuilder builder,
        PendingStore store,
        UsagePublisher publisher,
        SourceIdentity identity,
        ThreadPool threadPool,
        Clock clock
    ) {
        return NoopUsageReporter.INSTANCE;
    }
}
