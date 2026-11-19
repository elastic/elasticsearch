/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import java.util.List;

/**
 * A {@link UsagePublisher} that sends nothing: it accepts nothing and fails nothing, so records stay pending.
 */
public final class NoopUsagePublisher implements UsagePublisher {

    public static final NoopUsagePublisher INSTANCE = new NoopUsagePublisher();

    private NoopUsagePublisher() {}

    @Override
    public PublishResult publish(List<UsageRecord> records) {
        return PublishResult.empty();
    }
}
