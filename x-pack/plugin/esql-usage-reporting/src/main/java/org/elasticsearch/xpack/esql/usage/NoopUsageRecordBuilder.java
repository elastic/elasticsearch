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
 * A {@link UsageRecordBuilder} that builds nothing, so that the reporter can run, and report nothing, before the real builder
 * exists.
 */
public final class NoopUsageRecordBuilder implements UsageRecordBuilder {

    public static final NoopUsageRecordBuilder INSTANCE = new NoopUsageRecordBuilder();

    private NoopUsageRecordBuilder() {}

    @Override
    public List<UsageRecord> build(UsageSnapshot snapshot, SourceIdentity identity, Duration period, Instant creationTime) {
        return List.of();
    }
}
