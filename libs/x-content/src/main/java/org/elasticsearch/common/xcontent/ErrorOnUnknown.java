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

package org.elasticsearch.common.xcontent;

import java.util.ServiceLoader;

/**
 * Extension point to customize the error message for unknown fields. We expect
 * Elasticsearch to plug a fancy implementation that uses Lucene's spelling
 * correction infrastructure to suggest corrections.
 */
public interface ErrorOnUnknown {
    /**
     * The implementation of this interface that was loaded from SPI.
     */
    ErrorOnUnknown IMPLEMENTATION = findImplementation();

    /**
     * Build the error message to use when {@link ObjectParser} encounters an unknown field.
     * @param parserName the name of the thing we're parsing
     * @param unknownField the field that we couldn't recognize
     * @param candidates the possible fields
     */
    String errorMessage(String parserName, String unknownField, Iterable<String> candidates);

    /**
     * Priority that this error message handler should be used.
     */
    int priority();

    private static ErrorOnUnknown findImplementation() {
        ErrorOnUnknown best = new ErrorOnUnknown() {
            @Override
            public String errorMessage(String parserName, String unknownField, Iterable<String> candidates) {
                return "[" + parserName + "] unknown field [" + unknownField + "]";
            }

            @Override
            public int priority() {
                return Integer.MIN_VALUE;
            }
        };
        for (ErrorOnUnknown c : ServiceLoader.load(ErrorOnUnknown.class)) {
            if (best.priority() < c.priority()) {
                best = c;
            }
        }
        return best;
    }
}
