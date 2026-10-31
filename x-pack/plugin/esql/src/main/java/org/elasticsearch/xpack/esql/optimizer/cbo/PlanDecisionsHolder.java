/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Per-query slot for decisions produced after the physical optimizer is constructed. */
public final class PlanDecisionsHolder {
    private final AtomicReference<PlanDecisions> current = new AtomicReference<>(PlanDecisions.EMPTY);

    /** Reads the latest plan choices from mapper and physical optimizer phases. */
    public PlanDecisions get() {
        return current.get();
    }

    /** Publishes choices after the physical optimizer has been created. */
    public void set(PlanDecisions decisions) {
        current.set(Objects.requireNonNull(decisions));
    }
}
