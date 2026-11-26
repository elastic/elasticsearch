/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import java.time.Instant;

/**
 * A {@link UsageReporter} that does nothing, used until the real reporter exists.
 */
public final class NoopUsageReporter implements UsageReporter {

    public static final NoopUsageReporter INSTANCE = new NoopUsageReporter();

    private NoopUsageReporter() {}

    @Override
    public void start() {}

    @Override
    public void tick(Instant now) {}

    @Override
    public void close() {}
}
