/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.EnumSet;
import java.util.Optional;

/** Base for golden tests that run with both unmapped_fields=nullify and unmapped_fields=load. */
public abstract class UnmappedGoldenTestCase extends GoldenTestCase {
    /** Runs the query with both {@code NULLIFY} and {@code LOAD}; throws if either fails. */
    protected void runTestsNullifyAndLoad(String query, EnumSet<Stage> stages) {
        Optional<Throwable> nullifyException = tryRunTestsNullifyOnly(query, stages);
        Optional<Throwable> loadException = tryRunTestsLoadOnly(query, stages);
        nullifyException.ifPresent(e -> {
            throw new RuntimeException(
                loadException.isPresent() ? "Both nullify and load modes failed" : "Nullify mode failed (but load succeeded)",
                e
            );
        });
        loadException.ifPresent(e -> { throw new RuntimeException("Load mode failed (but nullify succeeded)", e); });
    }

    protected void runTestsNullifyOnly(String query, EnumSet<Stage> stages) {
        tryRunTestsNullifyOnly(query, stages).ifPresent(e -> { throw new RuntimeException("Nullify mode failed", e); });
    }

    protected void runTestsLoadOnly(String query, EnumSet<Stage> stages) {
        tryRunTestsLoadOnly(query, stages).ifPresent(e -> { throw new RuntimeException("Load mode failed", e); });
    }

    private Optional<Throwable> tryRunTestsNullifyOnly(String query, EnumSet<Stage> stages) {
        return builder(setUnmappedNullify(query)).nestedPath("nullify").stages(stages).tryRun();
    }

    private Optional<Throwable> tryRunTestsLoadOnly(String query, EnumSet<Stage> stages) {
        return EsqlCapabilities.Cap.OPTIONAL_FIELDS_V2.isEnabled()
            ? builder(setUnmappedLoad(query)).nestedPath("load").stages(stages).tryRun()
            : Optional.empty();
    }

    private static String setUnmappedNullify(String query) {
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    private static String setUnmappedLoad(String query) {
        return "SET unmapped_fields=\"load\"; " + query;
    }
}
