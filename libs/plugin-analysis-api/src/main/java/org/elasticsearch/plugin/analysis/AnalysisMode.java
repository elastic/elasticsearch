/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis;

/**
 * Enum representing the mode in which token filters and analyzers are allowed to operate.
 * While most token filters are allowed both in index and search time analyzers, some are
 * restricted to be used only at index time, others at search time.
 */
public enum AnalysisMode {

    /**
     * AnalysisMode representing analysis components that can be used only at index time.
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
     * AnalysisMode representing analysis components that can be used only at search time.
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
     * AnalysisMode representing analysis components that can be used both at index and search time.
     */
    ALL("all") {
        @Override
        public AnalysisMode merge(AnalysisMode other) {
            return other;
        }
    };

    private final String readableName;

    AnalysisMode(String name) {
        this.readableName = name;
    }

    /**
     * Returns a readable name of the analysis mode.
     * @return a name of the analysis mode.
     */
    public String getReadableName() {
        return this.readableName;
    }

    /**
     * Returns a mode that is compatible with both this mode and the other mode, that is:
     * <ul>
     * <li>ALL.merge(INDEX_TIME) == INDEX_TIME</li>
     * <li>ALL.merge(SEARCH_TIME) == SEARCH_TIME</li>
     * <li>INDEX_TIME.merge(SEARCH_TIME) throws an {@link IllegalStateException}</li>
     * </ul>
     * @return merged AnalysisMode
     * @throws IllegalStateException when  {@link #INDEX_TIME} is merged with {@link #SEARCH_TIME} (and vice versa)
     */
    public abstract AnalysisMode merge(AnalysisMode other);
}
