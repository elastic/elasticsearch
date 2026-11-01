/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.ToLongFunction;

/**
 * Replaces the estimate a parsed object was charged with the heap it retains, see {@link SizedParsedObject}. Implemented by the parsers of
 * a request's content and by parsers that wrap them, so that nested objects are tracked too.
 */
public interface ParsedObjectTracker {

    /**
     * Parses an object with {@code parse}, then charges the size {@code retainedBytes} reports instead of the estimate for its tokens. A
     * negative size, such as {@link SizedParsedObject#UNKNOWN}, keeps the estimate. Objects tracked inside it keep their own charges, so
     * the size must not count them.
     */
    <T> T parseSized(CheckedSupplier<T, IOException> parse, ToLongFunction<? super T> retainedBytes) throws IOException;

    /**
     * Parses objects without charging anything for them.
     */
    ParsedObjectTracker NONE = new ParsedObjectTracker() {
        @Override
        public <T> T parseSized(CheckedSupplier<T, IOException> parse, ToLongFunction<? super T> retainedBytes) throws IOException {
            return parse.get();
        }
    };

    /**
     * The tracker of {@code parser}, or {@link #NONE} if it is not charged.
     */
    static ParsedObjectTracker of(XContentParser parser) {
        return parser instanceof ParsedObjectTracker tracker ? tracker : NONE;
    }
}
