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

import org.elasticsearch.script.ScriptException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static java.util.Collections.singletonList;

/**
 * Utility methods for debugging painless scripts that are accessible to painless scripts.
 */
public class Debug {
    private Debug() {}

    /**
     * Throw an {@link Error} that "explains" an object.
     */
    public static void explain(Object objectToExplain) throws PainlessExplainError {
        throw new PainlessExplainError(objectToExplain);
    }

    /**
     * Thrown by {@link Debug#explain(Object)} to explain an object. Subclass of {@linkplain Error} so it cannot be caught by painless
     * scripts.
     */
    public static class PainlessExplainError extends Error {
        private final Object objectToExplain;

        public PainlessExplainError(Object objectToExplain) {
            this.objectToExplain = objectToExplain;
        }

        Object getObjectToExplain() {
            return objectToExplain;
        }

        /**
         * Headers to be added to the {@link ScriptException} for structured rendering.
         */
        Map<String, List<String>> getMetadata() {
            Map<String, List<String>> metadata = new TreeMap<>();
            metadata.put("es.class", singletonList(objectToExplain == null ? "null" : objectToExplain.getClass().getName()));
            metadata.put("es.to_string", singletonList(Objects.toString(objectToExplain)));
            return metadata;
        }
    }
}
