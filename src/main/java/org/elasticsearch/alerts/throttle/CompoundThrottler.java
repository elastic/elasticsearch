/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.throttle;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.common.collect.ImmutableList;

import java.util.List;

/**
 *
 */
public class CompoundThrottler implements Throttler {

    private final List<Throttler> throttlers;

    public CompoundThrottler(List<Throttler> throttlers) {
        this.throttlers = throttlers;
    }

    @Override
    public Result throttle(Alert alert, Trigger.Result result) {
        for (Throttler throttler : throttlers) {
            Result rslt = throttler.throttle(alert, result);
            if (rslt.throttle()) {
                return rslt;
            }
        }
        return Result.NO;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final ImmutableList.Builder<Throttler> throttlers = ImmutableList.builder();

        private Builder() {
        }

        public Builder add(Throttler throttler) {
            throttlers.add(throttler);
            return this;
        }

        public Throttler build() {
            ImmutableList<Throttler> list = throttlers.build();
            return list.isEmpty() ? Throttler.NO_THROTTLE : new CompoundThrottler(list);
        }
    }
}
