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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;

/**
 * Context of an operation that uses scripts as part of its execution.
 */
public interface ScriptContext {

    /**
     * @return the name of the operation
     */
    String getKey();

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
        public String getKey() {
            return key;
        }

        @Override
        public String toString() {
            return getKey();
        }
    }

    /**
     * Custom operation exposed via plugin, which makes use of scripts as part of its execution
     */
    final class Plugin implements ScriptContext {

        private final String pluginName;
        private final String operation;
        private final String key;

        /**
         * Creates a new custom scripts based operation exposed via plugin.
         * The name of the plugin combined with the operation name can be used to enable/disable scripts via fine-grained settings.
         *
         * @param pluginName the name of the plugin
         * @param operation the name of the operation
         */
        public Plugin(String pluginName, String operation) {
            if (Strings.hasLength(pluginName) == false) {
                throw new ElasticsearchIllegalArgumentException("plugin name cannot be empty when registering a custom script context");
            }
            if (Strings.hasLength(operation) == false) {
                throw new ElasticsearchIllegalArgumentException("operation name cannot be empty when registering a custom script context");
            }
            this.pluginName = pluginName;
            this.operation = operation;
            this.key = pluginName + "_" + operation;
        }

        public final String getPluginName() {
            return pluginName;
        }

        public final String getOperation() {
            return operation;
        }

        @Override
        public final String getKey() {
            return key;
        }

        @Override
        public final String toString() {
            return getKey();
        }
    }
}
