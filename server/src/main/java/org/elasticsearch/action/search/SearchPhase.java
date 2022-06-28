/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.core.CheckedRunnable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Base class for all individual search phases like collecting distributed frequencies, fetching documents, querying shards.
 */
abstract class SearchPhase implements CheckedRunnable<IOException> {
    private final String name;

    protected SearchPhase(String name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    /**
     * Returns the phases name.
     */
    public String getName() {
        return name;
    }

    public void start() {
        try {
            run();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
