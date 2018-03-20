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

/**
 * Helpers for dealing with boolean values. Package-visible only so that only XContent classes use them.
 */
final class Booleans {
    /**
     * Parse {@code value} with values "true", "false", or null, returning the
     * default value if null or the empty string is used. Any other input
     * results in an {@link IllegalArgumentException} being thrown.
     */
    static boolean parseBoolean(String value, Boolean defaultValue) {
        if (value != null && value.length() > 0) {
            switch (value) {
                case "true":
                    return true;
                case "false":
                    return false;
                default:
                    throw new IllegalArgumentException("Failed to parse param [" + value + "] as only [true] or [false] are allowed.");
            }
        } else {
            return defaultValue;
        }
    }

}
