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

package org.elasticsearch.script;

/**
 * Context of an operation that uses scripts as part of its execution.
 */
public interface ScriptContext {

    /**
     * @return the name of the operation
     */
    String key();

    /**
     * Standard operations that make use of scripts as part of their execution.
     * Note that the suggest api is considered part of search for simplicity, as well as the percolate api.
     */
    enum Standard implements ScriptContext {

        AGGS("aggs"), MAPPING("mapping"), SEARCH("search"), UPDATE("update");

        private final String key;

        Standard(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }
    }

    /**
     * Generic custom operation exposed via plugin
     *
     * @deprecated create a new {@link org.elasticsearch.script.ScriptContext.Plugin} instance instead
     */
    @Deprecated
    Plugin GENERIC_PLUGIN = new Plugin("plugin");

    /**
     * Custom operation exposed via plugin, which makes use of scripts as part of its execution
     */
    class Plugin implements ScriptContext {

        private final String key;

        /**
         * Creates a new custom scripts based operation exposed via plugin
         * @param key the name of the operation. can be used to enable/disable scripts via fine-grained settings
         */
        public Plugin(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }
    }
}
