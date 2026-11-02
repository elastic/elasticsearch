/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.TransportVersion;

import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * The two runs of a version-parametrized planner test suite. {@code current} pins {@link TransportVersion#current()} so a
 * version-gated plan change surfaces there; {@code historical} draws a random compatible version so the old shape stays
 * covered. {@link #toString()} is the parameter name shown in the test name.
 */
public enum VersionMode {
    CURRENT(TransportVersion::current),
    HISTORICAL(EsqlTestUtils::randomMinimumVersion);

    private final Supplier<TransportVersion> version;

    VersionMode(Supplier<TransportVersion> version) {
        this.version = version;
    }

    /** A fresh draw for this mode; {@code current} always answers the same. */
    public TransportVersion version() {
        return version.get();
    }

    /** The {@code @ParametersFactory} rows, one per mode. */
    public static List<Object[]> params() {
        return List.of(new Object[] { CURRENT }, new Object[] { HISTORICAL });
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
