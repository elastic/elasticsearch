/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

/**
 * Enum representing the mode in which token filters and analyzers are allowed to operate.
 * While most token filters are allowed both in index and search time analyzers, some are
 * restricted to be used only at index time, others at search time.
 */
public enum AnalysisMode {

    /**
     * AnalysisMode representing analysis components that can be used only at index time
     */
    INDEX_TIME("index time") {
        @Override
        public AnalysisMode merge(AnalysisMode other) {
            if (other == AnalysisMode.SEARCH_TIME) {
                throw new IllegalStateException("Cannot merge SEARCH_TIME and INDEX_TIME analysis mode.");
            }
            return AnalysisMode.INDEX_TIME;
        }
    },
    /**
     * AnalysisMode representing analysis components that can be used only at search time
     */
    SEARCH_TIME("search time") {
        @Override
        public AnalysisMode merge(AnalysisMode other) {
            if (other == AnalysisMode.INDEX_TIME) {
                throw new IllegalStateException("Cannot merge SEARCH_TIME and INDEX_TIME analysis mode.");
            }
            return AnalysisMode.SEARCH_TIME;
        }
    },
    /**
     * AnalysisMode representing analysis components that can be used both at index and search time
     */
    ALL("all") {
        @Override
        public AnalysisMode merge(AnalysisMode other) {
            return other;
        }
    };

    private String readableName;

    AnalysisMode(String name) {
        this.readableName = name;
    }

    /**
     * Retrieves the human-readable name of this analysis mode.
     *
     * @return the readable name (e.g., "index time", "search time", "all")
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnalysisMode mode = AnalysisMode.INDEX_TIME;
     * String name = mode.getReadableName(); // Returns "index time"
     * }</pre>
     */
    public String getReadableName() {
        return this.readableName;
    }

    /**
     * Merges this analysis mode with another mode, returning a mode compatible with both.
     * The merge rules are:
     * <ul>
     * <li>ALL.merge(INDEX_TIME) returns INDEX_TIME</li>
     * <li>ALL.merge(SEARCH_TIME) returns SEARCH_TIME</li>
     * <li>INDEX_TIME.merge(SEARCH_TIME) throws an {@link IllegalStateException}</li>
     * <li>SEARCH_TIME.merge(INDEX_TIME) throws an {@link IllegalStateException}</li>
     * </ul>
     *
     * @param other the analysis mode to merge with
     * @return the merged analysis mode
     * @throws IllegalStateException if attempting to merge incompatible modes (INDEX_TIME with SEARCH_TIME)
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnalysisMode merged = AnalysisMode.ALL.merge(AnalysisMode.INDEX_TIME);
     * // merged is INDEX_TIME
     *
     * // This will throw IllegalStateException:
     * AnalysisMode invalid = AnalysisMode.INDEX_TIME.merge(AnalysisMode.SEARCH_TIME);
     * }</pre>
     */
    public abstract AnalysisMode merge(AnalysisMode other);
}
