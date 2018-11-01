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

package org.elasticsearch.common;

import org.elasticsearch.ElasticsearchException;

import java.io.UnsupportedEncodingException;
import java.util.function.BiFunction;

/**
 * A set of utilities for names.
 */
public class Names {

    public static final int MAX_INDEX_NAME_BYTES = 255;

    private Names() {}

    /**
     * Validates a name by the rules used for Index names. Used for some things that are not indexes for consistency.
     */
    public static void validateNameWithIndexNameRules(String name, BiFunction<String, String, ? extends RuntimeException> exceptionCtor) {
        if (!Strings.validFileName(name)) {
            throw exceptionCtor.apply(name, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (name.contains("#")) {
            throw exceptionCtor.apply(name, "must not contain '#'");
        }
        if (name.contains(":")) {
            throw exceptionCtor.apply(name, "must not contain ':'");
        }
        if (name.charAt(0) == '_' || name.charAt(0) == '-' || name.charAt(0) == '+') {
            throw exceptionCtor.apply(name, "must not start with '_', '-', or '+'");
        }
        int byteCount = 0;
        try {
            byteCount = name.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new ElasticsearchException("Unable to determine length of name", e);
        }
        if (byteCount > MAX_INDEX_NAME_BYTES) {
            throw exceptionCtor.apply(name, "name is too long, (" + byteCount + " > " + MAX_INDEX_NAME_BYTES + ")");
        }
        if (name.equals(".") || name.equals("..")) {
            throw exceptionCtor.apply(name, "must not be '.' or '..'");
        }
    }
}
