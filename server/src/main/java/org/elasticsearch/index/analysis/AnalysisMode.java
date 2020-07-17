/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
     */
    public abstract AnalysisMode merge(AnalysisMode other);
}
