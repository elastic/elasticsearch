/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

/**
 * Tri-state result of {@link DataSourceModule#testConnection}.
 * <ul>
 *   <li>{@link Success} — the probe ran and the backend is reachable.</li>
 *   <li>{@link Failure} — the probe ran but the backend rejected or was unreachable.</li>
 *   <li>{@link Untestable} — the type is registered and valid but has no connectivity probe.</li>
 * </ul>
 */
public sealed interface TestConnectionResult permits TestConnectionResult.Success, TestConnectionResult.Failure,
    TestConnectionResult.Untestable {

    /** Probe succeeded. */
    record Success() implements TestConnectionResult {}

    /** Probe ran but failed; carries a human-readable reason. */
    record Failure(String error) implements TestConnectionResult {}

    /** Type is valid but cannot be probed. */
    record Untestable() implements TestConnectionResult {}

    TestConnectionResult SUCCESS = new Success();
    TestConnectionResult UNTESTABLE = new Untestable();

    static TestConnectionResult failure(String error) {
        return new Failure(error);
    }
}
