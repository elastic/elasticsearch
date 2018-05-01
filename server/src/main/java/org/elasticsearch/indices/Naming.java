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
package org.elasticsearch.indices;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;

import java.io.UnsupportedEncodingException;
import java.util.Locale;

/**
 * Used to validate numerous index and related artifact names.
 */
public enum Naming {

    INDEX("index") {
        void validate0(final String index) throws InvalidIndexNameException {
            if (!index.toLowerCase(Locale.ROOT).equals(index)) {
                this.report(index, "must be lowercase");
            }
        }

        void report(final String name, final String message) throws InvalidIndexNameException {
            throw new InvalidIndexNameException(name, message);
        }
    },
    ALIAS("alias"){
        void validate0(final String index) throws InvalidIndexNameException {
            // nop
        }

        void report(final String name, final String message) throws InvalidAliasNameException {
            throw new InvalidAliasNameException(name, message);
        }
    },
    TEMPLATE("template"){
        void validate0(final String index) throws InvalidIndexNameException {
            // nop
        }

        void report(final String name, final String message) throws InvalidTemplateNameException {
            throw new InvalidTemplateNameException(name, message);
        }
    };

    /**
     * This name will be used in messages, that mention the artifacts name explicitly.
     */
    private String prettyName;

    Naming(final String prettyName) {
        this.prettyName = prettyName;
    }

    public static final int MAX_NAME_LENGTH_BYTES = 255;

    /**
     * Validate the name for an index or alias against some static rules.
     */
    public final void validate(String name) {
        if (!Strings.validFileName(name)) {
            report(name, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (name.contains("#")) {
            report(name, "must not contain '#'");
        }
        if (name.contains(":")) {
            report(name, "must not contain ':'");
        }
        final char first = name.charAt(0);
        if (first == '_' || first == '-' || first == '+') {
            report(name, "must not start with '_', '-', or '+'");
        }
        int byteCount = 0;
        try {
            byteCount = name.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new ElasticsearchException("Unable to determine length of " + this.prettyName + " name", e);
        }
        if (byteCount > MAX_NAME_LENGTH_BYTES) {
            report(name, this.prettyName + " name is too long, (" + byteCount + " > " + MAX_NAME_LENGTH_BYTES + ")");
        }
        if (name.equals(".") || name.equals("..")) {
            report(name, "must not be '.' or '..'");
        }
        this.validate0(name);
    }

    abstract void validate0(String name);

    abstract void report(String name, String message) throws InvalidNameException;
}
