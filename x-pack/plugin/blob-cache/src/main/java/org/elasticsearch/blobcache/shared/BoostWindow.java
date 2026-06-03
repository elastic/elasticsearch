/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

/**
 * BoostWindow defines the base configuration for a time window. It also stores overrides
 * so that the methods, e.g. cachedRatio, return values taking the overrides into account.
 */
public class BoostWindow {

    private static final int CACHE_POWER_FOR_FULLY_CACHED = 200;
    private static final double DEFAULT_BOOST_FACTOR = 1.0;

    private final String name; // an arbitrary identifier for the boost window
    private final TimeValue maxAge; // there is no minAge, the window covers everything younger than maxAge
    private final int cachePowerMin; // min for now, but can be a range in future

    @Nullable
    private final Overrides overrides;

    public BoostWindow(String name, TimeValue maxAge, int cachePowerMin) {
        this(name, maxAge, cachePowerMin, null);
    }

    public BoostWindow(String name, TimeValue maxAge, int cachePowerMin, @Nullable Overrides overrides) {
        assert cachePowerMin >= 0;
        // -1 for indices with no timestamp
        assert maxAge != null && maxAge.getMillis() >= -1 : "invalid maxAge: " + maxAge;
        // boost factor must be non-negative
        assert overrides == null || overrides.boostFactorMin() >= 0.0 : "invalid overrides: " + overrides;
        // cannot override maxAge for indices with no timestamp
        assert overrides == null || overrides.maxAge() == null || overrides.maxAge().getMillis() >= 0 : "invalid overrides: " + overrides;

        this.name = name;
        this.cachePowerMin = cachePowerMin;
        this.maxAge = maxAge;
        this.overrides = overrides;
    }

    public String name() {
        return name;
    }

    public double cachedRatio() {
        // TODO: when we have cachePowerMax, we can choose a value between min and max based on the timestamp
        double boostFactorMin = overrides != null ? overrides.boostFactorMin() : DEFAULT_BOOST_FACTOR;
        return (cachePowerMin * boostFactorMin) / CACHE_POWER_FOR_FULLY_CACHED;
    }

    public TimeValue effectiveMaxAge() {
        TimeValue overrideMaxAge = overrides != null ? overrides.maxAge() : null;
        return overrideMaxAge != null ? overrideMaxAge : maxAge;
    }

    /**
     * This method creates a new BoostWindow by using the passed-in overrides, which is meant to be
     * finer grained, e.g. index level overrides, while the existing one is coarser grained, e.g.
     * project wide or a single configuration wide (e.g. `logs`).
     */
    public BoostWindow withOverrides(@Nullable Overrides overrides) {
        if (overrides == null) {
            return this;
        }
        // This simply drops old overrides (if exists) and honor the new ones
        return new BoostWindow(name, maxAge, cachePowerMin, overrides);
    }

    @Override
    public String toString() {
        return "BoostWindow{"
            + "name='"
            + name
            + '\''
            + ", cachePowerMin="
            + cachePowerMin
            + ", maxAge="
            + maxAge
            + ", overrides="
            + overrides
            + '}';
    }

    public record Overrides(
        TimeValue maxAge,
        double boostFactorMin // min for now, but maybe a range in future
    ) {}
}
