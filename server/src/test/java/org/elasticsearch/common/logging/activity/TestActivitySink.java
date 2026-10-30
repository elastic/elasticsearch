/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * An {@link ActivitySink} for tests whose switch and behaviour are supplied by the test, as {@link ActivitySink} is abstract.
 */
class TestActivitySink<C extends ActivityCaptureContext> extends ActivitySink<C> {
    private final BooleanSupplier enabled;
    private final Consumer<C> consumer;

    TestActivitySink(BooleanSupplier enabled, Consumer<C> consumer, Supplier<String> description) {
        super(description);
        this.enabled = enabled;
        this.consumer = consumer;
    }

    @Override
    public boolean isEnabled() {
        return enabled.getAsBoolean();
    }

    @Override
    public void accept(C context) {
        consumer.accept(context);
    }
}
