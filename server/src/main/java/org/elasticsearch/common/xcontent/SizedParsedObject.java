/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

/**
 * Implemented by a parsed object that can tell the heap it retains. While it is parsed, the request is charged an estimate from its
 * tokens, and once it is parsed, the size it reports instead, see {@link ParsedObjectTracker}. Objects that do not implement this keep the
 * estimate.
 */
public interface SizedParsedObject {

    /**
     * Reported by an object that holds something it cannot size. The estimate is kept.
     */
    long UNKNOWN = -1;

    /**
     * The heap this object retains, or {@link #UNKNOWN}. Called once, right after the object is parsed. It leaves out objects tracked
     * inside it, such as the clauses of a compound query, which report their own sizes, and the strings of object keys, which the parser
     * charges. A field name given as a value, such as the {@code field} of a kNN search, is counted.
     */
    long parsedRetainedBytes();

    /**
     * What {@code object} reports, or {@link #UNKNOWN} if it does not implement this interface.
     */
    static long of(Object object) {
        return object instanceof SizedParsedObject size ? size.parsedRetainedBytes() : UNKNOWN;
    }
}
