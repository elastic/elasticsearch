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

package org.elasticsearch.painless;

import java.util.BitSet;
import java.util.Set;

/**
 * Abstract superclass on top of which all Painless scripts are built.
 */
public abstract class PainlessScript {
    private final ScriptMetadata metadata;

    public PainlessScript(ScriptMetadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Get metadata about the script.
     */
    public ScriptMetadata getMetadata() {
        return metadata;
    }

    /**
     * Metadata about the script.
     */
    public static class ScriptMetadata {
        private final String name;
        private final String source;
        private final BitSet statements;
        private final Set<String> usedVariables;

        protected ScriptMetadata(String name, String source, BitSet statements, Set<String> usedVariables) {
            this.name = name;
            this.source = source;
            this.statements = statements;
            this.usedVariables = usedVariables;
        }

        /**
         * Name of the script set at compile time.
         */
        public String getName() {
            return name;
        }

        /**
         * Source of the script.
         */
        public String getSource() {
            return source;
        }

        /**
         * Finds the start of the first statement boundary that is on or before {@code offset}. If one is not found, {@code -1} is returned.
         */
        public int getPreviousStatement(int offset) {
            return statements.previousSetBit(offset);
        }

        /**
         * Finds the start of the first statement boundary that is after {@code offset}. If one is not found, {@code -1} is returned.
         */
        public int getNextStatement(int offset) {
            return statements.nextSetBit(offset + 1);
        }

        /**
         * Variables used by the script's main method including arguments. If an argument to the script isn't in this then it wasn't used by
         * the script and can safely be set to anything.
         */
        public Set<String> getUsedVariables() {
            return usedVariables;
        }
    }
}
